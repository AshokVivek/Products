from time import sleep

from app.database_utils import clickhouse_client, portal_db


async def fetch_org_data_and_update_transactions(entity_ids_list):
    entity_id_tuple = tuple(entity_ids_list) if len(entity_ids_list) > 1 else (entity_ids_list[0])
    print(f"Fetching data for the entities = {entity_id_tuple}")

    fetch_org_data_query = f"""
                            select 
                            distinct e.entity_id as entity_id,
                            o.id as org_id,
                            o.name as org_name
                            from bank_connect_entity e, finbox_dashboard_organization o
                            where e.organization_id=o.id
                            and e.entity_id in {entity_id_tuple}
                        """

    organization_data = await portal_db.fetch_all(query=fetch_org_data_query)
    if not organization_data:
        print(f"Data not found for entity ids = {entity_ids_list}")

    update_org_data_in_transactions(organization_data)
    sleep(0.5)


def update_org_data_in_transactions(organization_data):
    try:
        for org_data in organization_data:
            data = dict(org_data)
            org_id = data.get('org_id', None)
            org_name = data.get('org_name', None)
            entity_id = data.get('entity_id', None)

            if not (org_id or org_name or entity_id):
                print(f"Data Missing: org_id={org_id}, org_name={org_name}, entity_id={entity_id}")
                continue

            update_query = f"""
                             ALTER TABLE bank_connect.transactions
                             UPDATE org_id = '{org_id}', org_name='{org_name}'
                             WHERE org_name = ''
                             and entity_id = '{entity_id}'
                         """

            _ = clickhouse_client.query_df(update_query)
        print("Updated the transactions")
    except Exception as ex:
        print(f"Getting following error while updating the transaction table => {ex}")


async def update_transaction_table():
    entity_id_query = """
                        select distinct entity_id from bank_connect.transactions where org_name = ''
                      """
    entity_ids_data = clickhouse_client.query_df(entity_id_query)
    entity_ids_data.fillna('', inplace=True)
    print("Total count of distinct entities to be updated: ", len(entity_ids_data))

    entity_ids_list = list()

    for index, rows in entity_ids_data.iterrows():
        if not len(entity_ids_list) > 100:
            entity_ids_list.append(str(rows['entity_id']))
        else:
            await fetch_org_data_and_update_transactions(entity_ids_list)
            entity_ids_list = []

    if entity_ids_list:
        await fetch_org_data_and_update_transactions(entity_ids_list)
