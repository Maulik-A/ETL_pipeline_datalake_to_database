from sqlalchemy import create_engine, text
from .config import DB_URI


engine = create_engine(DB_URI)

def file1_merge_query(source_table, target_table):
    return f'''
    with cte as(
        select 
            MD5(CONCAT("Production Code","Unit ID")) as id,
            cast("Production Code" as varchar(5))as production_code,
            cast("Unit ID" as varchar(25))as unit_id,
            cast("Column A Stage A" as float) as column_a_stage_a,
            cast("Column B Stage A" as float) as column_b_stage_a,
            cast("Column C Stage A" as float) as column_c_stage_a,
            cast("Column D Stage A" as float) as column_d_stage_a,
            processing_ts
        from {source_table})
    MERGE into {target_table} target 
    using cte src
    on target.id = src.id
    when matched then
        update set 
        target.column_a_stage_a = src.column_a_stage_a,
        target.column_b_stage_a = src.column_b_stage_a,
        target.column_c_stage_a = src.column_c_stage_a,
        target.column_d_stage_a = src.column_d_stage_a
    when not matched then
        insert (id, production_code,unit_id,column_a_stage_a,column_b_stage_a, column_c_stage_a, column_d_stage_a,updated_at)	
        values(src.id, src.production_code,src.unit_id,src.column_a_stage_a,src.column_b_stage_a, src.column_c_stage_a, src.column_d_stage_a,current_timestamp())
    '''
def file3_merge_query(source_table, target_table):
    return f'''
    with cte as(
        select 
            MD5(CONCAT("Production Code","Parent ID","Child Position")) as id,
            cast("Production Code" as varchar(5))as production_code,
            cast("Parent ID" as varchar(25))as parent_id,
            cast("Child Position" as varchar(5)) as child_position,
            concat("Parent ID","Child Position") as unit_id,
            cast("Comment" as varchar(255)) as comment,
            cast("Column A Stage A" as float) as column_a_stage_a,
            cast("Column B Stage A" as float) as column_b_stage_a,
            cast("Column C Stage A" as float) as column_c_stage_a,
            processing_ts
        from {source_table}
    ),
    pivot as(
        select 
            id,
            production_code,
            parent_id,
            child_position,
            unit_id,
            AVG((column_a_stage_a + column_b_stage_a + column_c_stage_a) / 3.0) FILTER (WHERE comment = 'Comment') AS "comment",
            AVG((column_a_stage_a + column_b_stage_a + column_c_stage_a) / 3.0) FILTER (WHERE comment = 'test') AS "test",
            AVG((column_a_stage_a + column_b_stage_a + column_c_stage_a) / 3.0) FILTER (WHERE comment = 'This is not Real') AS "this_is_not_real"
            ,processing_ts
        FROM cte
        GROUP BY
            id,
            production_code,
            parent_id,
            child_position,
            unit_id,
            processing_ts)
    MERGE into {target_table} target 
    using pivot src
    on target.id = src.id
    when matched then
        update set 
        target.comment = src.comment,
        target.test = src.test,
        target.this_is_not_real = src.this_is_not_real
    when not matched then
        insert (id, production_code,parent_id,child_position,unit_id,comment, test, this_is_not_real,updated_at)	
        values(src.id, src.production_code,src.parent_id,src.child_position,src.unit_id,src.comment,src.test, src.this_is_not_real,current_timestamp())
    '''

def file2_merge_query(source_table, target_table):
    return f'''
    with cte as(
        select 
            MD5(CONCAT("Production Code","Parent ID","Child Position","Operator")) as id,
            cast("Production Code" as varchar(5))as production_code,
            cast("Parent ID" as varchar(25))as parent_id,
            cast("Child Position" as varchar(5)) as child_position,
            concat("Parent ID","Child Position") as unit_id,
            cast("Operator" as varchar(255)) as operator,
            cast("Column A Stage A" as float) as column_a_stage_a,
            processing_ts
        from {source_table})
    MERGE into {target_table} target 
    using cte src
    on target.id = src.id
    when matched then
        update set 
        target.column_a_stage_a = src.column_a_stage_a,
    when not matched then
        insert (id, production_code,parent_id,child_position,unit_id,operator,column_a_stage_a,,updated_at)	
        values(src.id, src.production_code,src.parent_id,src.child_position,src.unit_id,src.operator,src.column_a_stage_a,current_timestamp())
    '''


# with engine.connect() as conn:
#     conn.execute(text(file1_query))



def load_to_prod(source_table:str, target_table:str, query_func:function) -> None:
    query = query_func(source_table,target_table)
    with engine.connect() as conn:
        conn.execute(text(query))
