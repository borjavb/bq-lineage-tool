package com.borjav.data.parser;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.zetasql.Analyzer.extractTableNamesFromNextStatement;

import com.borjav.data.model.ResolvedNodeExtended;
import com.borjav.data.options.Options;
import com.borjav.data.service.BigQueryZetaSqlSchemaLoader;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.ParseResumeLocation;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.ZetaSQLBuiltinFunctionOptions;
import com.google.zetasql.ZetaSQLOptions;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

public class ZetaSQLResolver {

  private final BigQueryZetaSqlSchemaLoader tableSchemaLoader;
  private SimpleCatalog catalog;

  public ZetaSQLResolver(@Nullable BigQueryZetaSqlSchemaLoader tableSchemaLoader) {
    this.catalog = new SimpleCatalog("data-catalog");
    catalog.addZetaSQLFunctionsAndTypes(
        new ZetaSQLBuiltinFunctionOptions(enableAllLanguageFeatures()));
    this.tableSchemaLoader = tableSchemaLoader;
  }

  public ZetaSQLResolver(SimpleCatalog catalog) {
    this.catalog = catalog;
    catalog.addZetaSQLFunctionsAndTypes(
        new ZetaSQLBuiltinFunctionOptions(enableAllLanguageFeatures()));
    this.tableSchemaLoader = null;
  }

  private ImmutableSet<String> extractReferencedTables(ParseResumeLocation aParseResumeLocation,
                                                       AnalyzerOptions analyzerOptions) {

    return extractTableNamesFromNextStatement(aParseResumeLocation, analyzerOptions).stream()
        .map(k -> StringUtils.join(k, "."))
        .collect(toImmutableSet());
  }


  public String replaceNotFullyQualifiedTables(String sql) {
    if (Options.missing_project.size() >= 1) {
      Iterator<Map.Entry<String, String>> it = Options.missing_project.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pair = (Map.Entry) it.next();
        sql = sql.replaceAll("`?" + pair.getValue().toString().replace(".", "\\.") + "`?",
            "`" + pair.getKey().toString() + "`");
        it.remove(); // avoids a ConcurrentModificationException
      }
    }

    return sql;
  }


  public String replaceQuotesFullyQualifiedName(String sql) {
    return sql.replaceAll("`(.*)`\\.`(.*)`\\.`(.*)`", "`$1.$2.$3`");
  }


  public ResolvedNodeExtended extractLineage(String sql) {

    buildCatalogWithQueryTables(sql);

    ParseResumeLocation aParseResumeLocation = new ParseResumeLocation(sql);
    aParseResumeLocation.getInput();
    ResolvedNodeExtended finalTable = null;
    while (sql.getBytes().length > aParseResumeLocation.getBytePosition()) {
      try {
        ASTExplorer resolver = new ASTExplorer(this.catalog);
        finalTable = resolver.resolve(
            Analyzer.analyzeNextStatement(aParseResumeLocation, enableAllFeatures(), this.catalog));
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }

    return finalTable;
  }


  public ResolvedNodeExtended extractLineage(String sql, SimpleCatalog catalog) throws Exception {
    this.catalog = catalog;

    ParseResumeLocation aParseResumeLocation = new ParseResumeLocation(sql);
    aParseResumeLocation.getInput();
    ResolvedNodeExtended finalTable = null;

    while (sql.getBytes().length > aParseResumeLocation.getBytePosition()) {
      try {
        ASTExplorer resolver = new ASTExplorer(this.catalog);
        finalTable = resolver.resolve(
            Analyzer.analyzeNextStatement(aParseResumeLocation, enableAllFeatures(), this.catalog));
        final ResolvedNodes.ResolvedStatement stmt =
            Analyzer.analyzeNextStatement(aParseResumeLocation, enableAllFeatures(),
                this.catalog);
        Analyzer.buildStatement(stmt, this.catalog);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return finalTable;
  }

  public SimpleCatalog getCatalog() {

    return catalog;
  }


  private LanguageOptions enableAllLanguageFeatures() {
    LanguageOptions languageOptions = new LanguageOptions();
    languageOptions.setSupportsAllStatementKinds();
    languageOptions = languageOptions.enableMaximumLanguageFeatures();
    //usually some new syntax are not supported by the parser, so we need to enable them manually
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_CONCAT_MIXED_TYPES);
    languageOptions.enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_QUALIFY);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_ANALYTIC_FUNCTIONS);
    languageOptions.enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_EXTENDED_TYPES);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_DECIMAL_ALIAS);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_BETWEEN_UINT64_INT64);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_FORMAT_IN_CAST);
    languageOptions.enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_RANGE_TYPE);
    languageOptions.enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_INTERVAL_TYPE);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE);
    languageOptions.enableLanguageFeature(
        ZetaSQLOptions.LanguageFeature.FEATURE_V_1_4_GROUPING_SETS);

    // needed to enable qualify without the where clause
    languageOptions.enableReservableKeyword("QUALIFY");

    return languageOptions;
  }

  private AnalyzerOptions enableAllFeatures() {
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    // if false, the parser will extract all the columns from the referenced tables

    analyzerOptions.setLanguageOptions(enableAllLanguageFeatures());
    analyzerOptions.setPruneUnusedColumns(true);
    analyzerOptions.setAllowUndeclaredParameters(true);
    return analyzerOptions;
  }


  private void buildCatalogWithQueryTables(String sql) {

    if (tableSchemaLoader != null) {
      ParseResumeLocation aParseResumeLocation = new ParseResumeLocation(sql);
      while (sql.getBytes().length > aParseResumeLocation.getBytePosition()) {
        tableSchemaLoader.loadSchemas(
                extractReferencedTables(aParseResumeLocation, enableAllFeatures()))
            .forEach(k -> {
              if (catalog.getTable(k.getName(), null) == null) {
                catalog.addSimpleTable(k);
              }
            });
      }
    }
  }
}
