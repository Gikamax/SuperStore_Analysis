# ETL functions to use for Superstore analysis
from xmlrpc.client import Boolean
import pandas as pd
import sqlalchemy as db
from datetime import datetime, date
import hashlib
from dotenv import load_dotenv
import os

# Variables for connecting to Source and Database
load_dotenv()
db_connection_string = os.getenv("POSTGRES_DB") 

# Functions
def prepare_database(connection_string:str, schema_list:list) -> str:
    """
    Function to create the necessary Schema's in database. 
    """
    engine = db.create_engine(connection_string) # Create Engine 

    with engine.connect() as conn: # Connect to Database. 
        for schema in schema_list: # Iterate over the schema's 
            try: # Try if Schema not already exists
                conn.execute(db.schema.CreateSchema(schema.lower()))
                #print(f"{schema} created")
            except:
                pass
                #print(f"{schema} already exists")

def create_recordhash(columns):
    """
    Function to create RecordHash
    """
    md5 = hashlib.md5()
    for column in columns:  # for loop to go through all the columns
        # String of columns with sepration mark
        md5.update(str(column).encode("utf-8"))
    return md5.hexdigest()

def prepare_dataframe(dataframe:pd.DataFrame,table_name:str, unique_column:str) -> pd.DataFrame:
    """
    Adds all columns needed for DataVault 2.0 
    """
    # For creating record_hash all the columns must be made in same order
    columns_list = sorted([column for column in dataframe.columns])
    dataframe["record_hash"] = dataframe[columns_list].apply(create_recordhash, axis=1)  # recordhash

    #business key
    dataframe[f"{table_name}_BK"] = dataframe[unique_column].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())  # business key
    # Load Dts
    dataframe["load_dts"] = pd.to_datetime(datetime.today().strftime("%Y-%m-%d"))
    # LastSeen_dts
    dataframe["LastSeen_dts"] = pd.to_datetime(datetime.today().strftime("%Y-%m-%d"))
    # Record Source
    dataframe["record_source"] = "Test"
    return dataframe

def check_if_table_exists(table_name:str, type:str, connection_string:str) -> Boolean:
    """
    Checks if the Table already has a Hub or SAT. 
    """
    # Create engine
    engine = db.create_engine(connection_string)
    with engine.connect() as connection:
            metadata = db.MetaData()  # placeholder for Metadata
             # Retrive tables (information_schema.tables)
            tables = db.Table(
                 "tables",
                 metadata,
                 autoload=True,
                 autoload_with=engine,
                 schema="information_schema",
             )

             ## Create Query
            query = db.select([tables.columns.table_name])
            if type.lower() == "hub":
                # Where
                query = query.where(
                    db.and_(
                        tables.columns.table_name.like("HUB_%"),
                        tables.columns.table_schema == "dwh",
                    )
                )
            elif type.lower() == "lnk":
                # Where
                query = query.where(
                    db.and_(
                        tables.columns.table_name.like("LNK_%"),
                        tables.columns.table_schema == "dwh",
                    )
                )
            else:
                pass
             # Run Query
            table_list = connection.execute(query).fetchall()
         # Check if tablename is used
    for table in table_list:
        if table[0][4:].lower() == table_name.lower():
            return True
    return False

def setup_Analysis_Schema(connection_string:str, source_schema:str, target_schema:str):
    """
    Creates Analysis Schema.
    Removes all columns needed for DATAVAULT2.0.
    """
    # create Engine
    engine = db.create_engine(connection_string)
    # Find all relevant Views in DWH
    def get_source_views(schema_name:str) -> list:
        """
        INNER FUNCTION: retrieves all the views needed for converting to DM in a list. 
        """
        result_list = [] # Empty list for result
        with engine.connect() as connection:
            metadata = db.MetaData()  # placeholder for Metadata
            # Retrive tables (information_schema.tables)
            tables = db.Table(
                    "tables",
                        metadata,
                        autoload=True,
                        autoload_with=engine,
                        schema="information_schema",
                    )
            # Create Query
            query = db.select([tables.columns.table_name]) # Select only Table_name
            query = query.where(
                db.and_(
                    tables.columns.table_name.like("%_Currents"), # the Views with Currents, so *_Currents
                    tables.columns.table_type == "VIEW", # Type is View
                    tables.columns.table_schema == schema_name.lower() # for Schema DWH. 
                )
            )
            views_list = connection.execute(query).fetchall() # Execute query 
            for view in views_list: # Loop over the result
                result_list.append(view[0]) # Grab the first cause result = (table, )
        return result_list
    # Read in table and drop unnecessary columns
    def create_table_target(name_view:str, source_schema:str, target_schema:str):
        """
        INNER FUNCTION: Creates Schema with tables ready for analysis. 
        """
        columns_to_select = []
        dm_table_name = name_view.split('_')[1]
        with engine.connect() as connection:
            df = pd.read_sql(f'select * from {source_schema}."{name_view}"', connection) # Read in View

            for column in df.columns: # Iterate over all columns
                # Only Select Columns that are needed. 
                if column not in ['load_dts', 'LastSeen_dts', 'row_num', 'record_source', 'record_hash'] and column[-2:] != "BK": 
                    columns_to_select.append(column)

            df[columns_to_select].to_sql(dm_table_name,connection, target_schema, "replace") # Write to DM and replace if already present. 
    # Logic
    for _view in get_source_views(source_schema):
        create_table_target(_view, source_schema, target_schema)

def load_hub(dataframe:pd.DataFrame, table_name:str, unique_column:str, connection_string:str):
    """
    Function to Load the HUB and SAT combination. 
    Checks if the HUB and SAT exist, and acts accordingly. 
    dataframe           -> the table to be inserted. 
    table_name          -> Name of table (will look like HUB_table_name)
    unique_column       -> Unique Identifier of the table. 
    connection_string   -> The string to create SQL Alchemy connection.  
    """
    # Create engine
    engine = db.create_engine(connection_string)
    # Inner functions
    def create_hub(dataframe:pd.DataFrame, table_name:str, unique_column:str) -> pd.DataFrame:
        """
        INNER FUNCTION: Selects the right columns needed for hub. 
        """
        result = (
            dataframe[[f"{table_name}_BK", unique_column, "load_dts", "record_source"]]
            .groupby([f"{table_name}_BK", unique_column, "record_source"])
            .agg({"load_dts": "min"})
        )
        result.reset_index(inplace=True) # To unset the Group By
        return result

    def create_sat(dataframe:pd.DataFrame, table_name:str) -> pd.DataFrame:
        """
        INNER FUNCTION: Selects the right columns needed for sat.
        """
        return dataframe.drop_duplicates(subset=[f"{table_name}_BK", "load_dts"]) # To make sure the combination is unique

    def insert_hub(hub:pd.DataFrame, table_name:str):
        """
        INNER FUNCTION: Inserts the unseen rows in the hub. 
        """
        # Create engine
        with engine.connect() as connection:
            # Find existing hub
            hub_database = pd.read_sql_table(f"HUB_{table_name}", con=connection, schema="dwh")
            # Merge Hubs to find missing values
            result = hub.merge(hub_database[f"{table_name}_BK"], how="left", on=f"{table_name}_BK", indicator=True)
            # Insert database where _merge =="left_only"
            result[result["_merge"] == "left_only"].drop("_merge", axis=1).to_sql(f"HUB_{table_name}", con=connection, schema = "dwh", if_exists="append", index=False)
            #return f'Inserted {len(result[result["_merge"] == "left_only"])} rows' # To logging
            return 1

    def insert_sat(sat:pd.DataFrame, table_name:str) -> str:
        """
        INNER FUNCTION: Inserts unseen rows into SAT and updates rows still present. 
        """
        with engine.connect() as connection:
            # Load in current SAT from Database
            sat_database = pd.read_sql_table(f"SAT_{table_name}", con=connection, schema="dwh")

            # Find last version of inserted rows of the SAT
            sat_database = (sat_database[[f"{table_name}_BK", "record_hash", "load_dts"]]
                        .sort_values([f"{table_name}_BK", "load_dts"], ascending=False)
                        .groupby([f"{table_name}_BK"], as_index=False)
                        .head(1)
                        )
            
            # Find out if incoming data is already in SAT
            sat = sat.merge(
                        sat_database,
                        how="left",
                        on=[f"{table_name}_BK", "record_hash"],
                        indicator=True,
                        suffixes=["", "_Target"],
                    )
            
            # Insert Rows not Seen in DataBase
            sat[sat["_merge"] == "left_only"].drop(["_merge", "load_dts_Target"], axis=1).to_sql(
                        f"SAT_{table_name}",
                        con=connection,
                        schema="dwh",
                        if_exists="append",
                        index=False
                    )
            
            # Updating LastSeenDTS for values seen
                    # Getting all Loading Dates for records already present
            load_dts_lst = []
            for item in (sat[sat["_merge"] == "both"]["load_dts_Target"].astype(str).unique().tolist()):
                load_dts_lst.append(item)
            load_dts_tuple = tuple(load_dts_lst) + ("1900-01-01",)

            # Getting all Business Key's for records already present
            bk_lst = []
            for key in sat[sat["_merge"] == "both"][f"{table_name}_BK"].unique().tolist():
                bk_lst.append(key)
            bk_tuple = tuple(bk_lst)

            # Update the Last_seen_dts
            stmt = f"""
            Update dwh."SAT_{table_name}" 
            SET "LastSeen_dts" = '{date.today()}'
            WHERE "{table_name}_BK" in {bk_tuple}
            AND "load_dts" in {load_dts_tuple}
                    """
            connection.execute(stmt)
        #return f"Updated {len(bk_tuple)} rows" # Logging.
        return 1

    def create_hub_database(hub:pd.DataFrame, table_name:str) -> str:
        """
        INNER FUNCTION: if HUB not exists creates hub by inserting the HUB dataframe and altering table. 
        """
        # Set up Alter Statements
        hub_stmt = f"""ALTER TABLE dwh."HUB_{table_name}"
                            ADD CONSTRAINT PK_HUB_{table_name} PRIMARY KEY ("{table_name}_BK")
                            """

        with engine.connect() as connection:
            print(f"{table_name}_BK")
            hub.drop_duplicates(subset=[f"{table_name}_BK"], keep="first").to_sql(
                        f"HUB_{table_name}", 
                        con=connection, 
                        schema="dwh", 
                        index=False
                    )  # insert Hub
            connection.execute(hub_stmt)  # Alter HUB
        return f"Created and Inserted HUB_{table_name}"

    def create_sat_database(sat:pd.DataFrame, table_name) -> str:
        """
        INNER FUNCTION: if SAT not exists creates hub by inserting the SAT dataframe and altering table. 
        Futhermore, creates views based on SAT. 
        Actuals -> All rows with max LastSeen_Dts
        Current -> All latest rows inserted. 
        """
        # Create SAT Alter Statement
        sat_stmt = f"""ALTER TABLE dwh."SAT_{table_name}"
                                ADD CONSTRAINT PK_SAT_{table_name} PRIMARY KEY ("{table_name}_BK", load_dts);
                                ALTER TABLE dwh."SAT_{table_name}"
                                ADD CONSTRAINT FK_SAT_{table_name} FOREIGN KEY ("{table_name}_BK")
                                REFERENCES dwh."HUB_{table_name}" ("{table_name}_BK")                       
                                """
        # Statement for creation of Views
        vw_actual_stmt = f"""Create view dwh."vwSAT_{table_name}_Actuals" as 
                            select * from dwh."SAT_{table_name}"
                            where "LastSeen_dts" = 
                                (
                                    select max("LastSeen_dts") from dwh."SAT_{table_name}"
                                )                   
                        """
        vw_current_stmt = f""" 
                                create view dwh."vwSAT_{table_name}_Currents" as 
                                SELECT *
                                FROM (
                                    SELECT *
                                        ,ROW_NUMBER() OVER (
                                            PARTITION BY "{table_name}_BK" ORDER BY "load_dts" DESC
                                            ) row_num
                                    FROM dwh."vwSAT_{table_name}_Actuals"
                                    ) C
                                WHERE row_num = 1
                                
                                """

        with engine.connect() as connection:
                    sat.drop_duplicates(subset=["record_hash"]).to_sql(
                                    f"SAT_{table_name}", 
                                    con=connection, 
                                    schema="dwh", 
                                    index=False
                                )  # insert Sat
                    connection.execute(sat_stmt)  # Alter SAT

                    connection.execute(vw_actual_stmt)  # Create Actuals View
                    connection.execute(vw_current_stmt)  # Create Current stmt
        #return f"Created and Inserted SAT_{table_name}"
        return 1
    
    #Logic
    prepared_dataframe = prepare_dataframe(dataframe=dataframe, table_name=table_name, unique_column=unique_column) # Prepare the dataframe

    # Set the HUB and SAT
    hub = create_hub(prepared_dataframe, table_name=table_name, unique_column=unique_column)
    sat = create_sat(prepared_dataframe, table_name=table_name)

    # Check if table exists
    table_exists = check_if_table_exists(table_name=table_name, type="SAT", connection_string=connection_string)

    if table_exists: # if the table exists then we want to insert. 
        insert_hub(hub, table_name)
        insert_sat(sat, table_name)
    else: # if table not exists we want to create and alter. 
        create_hub_database(hub, table_name)
        create_sat_database(sat, table_name)

def load_lnk(dataframe:pd.DataFrame, table_name:str, unique_column:str, foreign_keys:dict, connection_string:str):
    """
    Function to Load the LNK and LSAT combination. 
    Checks if the LNK and LSAT exist, and acts accordingly. 
    dataframe           -> the table to be inserted. 
    table_name          -> Name of table (will look like LNK_table_name)
    unique_column       -> Unique Identifier of the table. 
    foreign_keys        -> Dict with Hub_name and foreign_key {Customer: Customer ID}
    connection_string   -> The string to create SQL Alchemy connection.  
    """
    # Create engine
    engine = db.create_engine(connection_string)
    # Internal Functions
    def create_lnk(dataframe:pd.DataFrame, table_name:str, unique_column:str, foreign_keys:dict) -> pd.DataFrame:
        """
        
        """
        # Create BusinessKeys from the SAT's based on foreign_keys
        for _table in foreign_keys:
            dataframe[f"{_table}_BK"] = dataframe[foreign_keys[_table]].apply( lambda x: hashlib.md5(str(x).encode()).hexdigest())
        # Define the columns for the LNK
        LNK_columns = (
            [f"{table_name}_BK", unique_column, "load_dts", "record_source"]
            + [_ for _ in foreign_keys.values()]
            + [f"{_}_BK" for _ in foreign_keys.keys()]
        )
        return dataframe[LNK_columns] # return dataframe with only those columns 

    def insert_lnk(lnk:pd.DataFrame, table_name:str):
            """
            INNER FUNCTION: Inserts the unseen rows in the LNK. 
            """
            # Create engine
            with engine.connect() as connection:
                # Find existing hub
                lnk_database = pd.read_sql_table(f"LNK_{table_name}", con=connection, schema="dwh")
                # Merge Hubs to find missing values
                result = lnk.merge(lnk_database[f"{table_name}_BK"], how="left", on=f"{table_name}_BK", indicator=True)
                # Insert database where _merge =="left_only"
                result[result["_merge"] == "left_only"].drop("_merge", axis=1).to_sql(f"LNK_{table_name}", con=connection, schema = "dwh", if_exists="append", index=False)
                #return f'Inserted {len(result[result["_merge"] == "left_only"])} rows' # To logging
                return 1
            
    def insert_lsat(lsat:pd.DataFrame, table_name:str) -> str:
            """
            INNER FUNCTION: Inserts unseen rows into LSAT and updates rows still present. 
            """
            with engine.connect() as connection:
                # Load in current SAT from Database
                lsat_database = pd.read_sql_table(f"LSAT_{table_name}", con=connection, schema="dwh")

                # Find last version of inserted rows of the SAT
                lsat_database = (lsat_database[[f"{table_name}_BK", "record_hash", "load_dts"]]
                            .sort_values([f"{table_name}_BK", "load_dts"], ascending=False)
                            .groupby([f"{table_name}_BK"], as_index=False)
                            .head(1)
                            )
                
                # Find out if incoming data is already in LSAT
                lsat = lsat.merge(
                            lsat_database,
                            how="left",
                            on=[f"{table_name}_BK", "record_hash"],
                            indicator=True,
                            suffixes=["", "_Target"],
                        )
                
                # Insert Rows not Seen in DataBase
                lsat[lsat["_merge"] == "left_only"].drop(["_merge", "load_dts_Target"], axis=1).to_sql(
                            f"LSAT_{table_name}",
                            con=connection,
                            schema="dwh",
                            if_exists="append",
                            index=False
                        )
                
                # Updating LastSeenDTS for values seen
                # Getting all Loading Dates for records already present
                load_dts_lst = []
                for item in (lsat[lsat["_merge"] == "both"]["load_dts_Target"].astype(str).unique().tolist()):
                    load_dts_lst.append(item)
                load_dts_tuple = tuple(load_dts_lst) + ("1900-01-01",)

                # Getting all Business Key's for records already present
                bk_lst = []
                for key in lsat[lsat["_merge"] == "both"][f"{table_name}_BK"].unique().tolist():
                    bk_lst.append(key)
                bk_tuple = tuple(bk_lst)

                # Update the Last_seen_dts
                stmt = f"""
                Update dwh."LSAT_{table_name}" 
                SET "LastSeen_dts" = '{date.today()}'
                WHERE "{table_name}_BK" in {bk_tuple}
                AND "load_dts" in {load_dts_tuple}
                        """
                connection.execute(stmt)
            #return f"Updated {len(bk_tuple)} rows" # Logging.
            return 1

    def create_lnk_database(lnk:pd.DataFrame, table_name:str,  foreign_keys:dict) -> str:
            """
            INNER FUNCTION: if LNK not exists creates lnk by inserting the LNK dataframe and altering table. 
            """
            # Set up Alter Statements
            lnk_stmt = f"""ALTER TABLE dwh."LNK_{table_name}"
                                ADD CONSTRAINT PK_LNK_{table_name} PRIMARY KEY ("{table_name}_BK");
                                """
            for table in foreign_keys.keys():
                    stmt = f"""ALTER TABLE dwh."LNK_{table_name}"
                    ADD CONSTRAINT FK_LNK_{table} FOREIGN KEY ("{table}_BK") 
                    REFERENCES dwh."HUB_{table}" ("{table}_BK");  
                    """
                    lnk_stmt += stmt
            with engine.connect() as connection:
                print(f"{table_name}_BK")
                lnk.drop_duplicates(subset=[f"{table_name}_BK"], keep="first").to_sql(
                            f"LNK_{table_name}", 
                            con=connection, 
                            schema="dwh", 
                            index=False
                        )  # insert Hub
                connection.execute(lnk_stmt)  # Alter LNK
            return f"Created and Inserted LNK_{table_name}"

    def create_lsat_database(lsat:pd.DataFrame, table_name) -> str:
        """
        INNER FUNCTION: if LSAT not exists creates LSAT by inserting the LSAT dataframe and altering table. 
        Futhermore, creates views based on LSAT. 
        Actuals -> All rows with max LastSeen_Dts
        Current -> All latest rows inserted. 
        """
        # Create SAT Alter Statement
        lsat_stmt = f"""ALTER TABLE dwh."LSAT_{table_name}"
                        ADD CONSTRAINT PK_LSAT_{table_name} PRIMARY KEY ("{table_name}_BK", load_dts);
                        ALTER TABLE dwh."LSAT_{table_name}"
                        ADD CONSTRAINT FK_LSAT_{table_name} FOREIGN KEY ("{table_name}_BK")
                        REFERENCES dwh."LNK_{table_name}" ("{table_name}_BK")                       
                                    """
        # Statement for creation of Views
        vw_actual_stmt = f"""Create view dwh."vwLSAT_{table_name}_Actuals" as 
                            select * from dwh."LSAT_{table_name}"
                            where "LastSeen_dts" = 
                                (
                                    select max("LastSeen_dts") from dwh."LSAT_{table_name}"
                                )                   
                            """
        vw_current_stmt = f""" 
                            create view dwh."vwLSAT_{table_name}_Currents" as 
                                SELECT *
                                FROM (
                                SELECT *
                                ,ROW_NUMBER() OVER (
                                PARTITION BY "{table_name}_BK" ORDER BY "load_dts" DESC
                                                ) row_num
                                        FROM dwh."vwLSAT_{table_name}_Actuals"
                                        ) C
                                    WHERE row_num = 1
                                    
                                    """
        with engine.connect() as connection:
                lsat.drop_duplicates(subset=["record_hash"]).to_sql(
                                        f"LSAT_{table_name}", 
                                        con=connection, 
                                        schema="dwh", 
                                        index=False
                                    )  # insert Sat
                connection.execute(lsat_stmt)  # Alter SAT

                connection.execute(vw_actual_stmt)  # Create Actuals View
                connection.execute(vw_current_stmt)  # Create Current stmt
            #return f"Created and Inserted SAT_{table_name}"
        return 1
    #Logic
    prepared_dataframe = prepare_dataframe(dataframe=dataframe, table_name=table_name, unique_column=unique_column) # prepare dataframe
    # Create LNK and LSAT
    lnk = create_lnk(dataframe=dataframe, table_name=table_name, unique_column=unique_column, foreign_keys=foreign_keys)
    lsat = dataframe
    # Check if Table exists
    table_exists = check_if_table_exists(table_name=table_name,type= "LNK", connection_string=connection_string)
    # Logic
    if table_exists: # if exists then insert
        insert_lnk(lnk, table_name)
        insert_lsat(lsat=lsat, table_name=table_name)
    else: # Not exits create 
        create_lnk_database(lnk=lnk, table_name=table_name, foreign_keys=foreign_keys)
        create_lsat_database(lsat=lsat, table_name=table_name)