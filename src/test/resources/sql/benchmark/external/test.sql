with tbl as (Select 1 as x,[struct(2 as y, [123,456,789] as z), struct(3,[301,302])] as lst),
    tbl2 as (Select x, A.y, z from tbl, unnest(lst) as A, unnest(A.z) as z)

    #select * from tbl2 # run this query first
    # Then this:
Select x, array_Agg(struct(y,Z))
from
    (
        select x,y,array_agg(z) as Z
        from tbl2
        group by 1,2
    )
group by 1