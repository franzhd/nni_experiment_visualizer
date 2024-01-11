import sqlite3
import json
from tabulate import tabulate

def merge_dicts(dict1, dict2)-> dict:
    merged_dict = {}
    for key in dict1.keys():
        merged_dict[key] = [dict1[key], dict2[key]]
    return merged_dict


def nni_query(tiral_sqlite_path, show=True) -> dict:

    # Connect to the SQLite database
    connection = sqlite3.connect(tiral_sqlite_path)
    cursor = connection.cursor()

    query = f"""
        SELECT trialjobId, sequence, data
        FROM MetricData
        WHERE trialjobId IN (
            SELECT trialjobId
            FROM MetricData
            WHERE type = 'FINAL'
            GROUP BY trialjobId
            ORDER BY MAX(data) DESC
            LIMIT 5
        ) 
        AND type = 'PERIODICAL'
        ORDER BY trialjobId, sequence;
    """


    # Execute the query
    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()
    print(results)
    score_dict = {}
    for row in results:
        trialjob_id = row[0]
        sequence = row[1]
        print(row[2])
        if isinstance(row[2].strip('"'), float):
            data = float(row[2].strip('"'))
        else:
            query = f"""
                    SELECT trialjobId, sequence, CAST(JSON_EXTRACT(data, '$.default') AS REAL) as metrics
                    FROM MetricData
                    WHERE trialjobId IN (
                        SELECT trialjobId
                        FROM MetricData
                        WHERE type = 'FINAL'
                        GROUP BY trialjobId
                        ORDER BY MAX(CAST(JSON_EXTRACT(data, '$.default') AS REAL)) DESC
                        LIMIT 5
                    ) 
                    AND type = 'PERIODICAL'
                    ORDER BY trialjobId, sequence;
                """
            # Execute the query
            cursor.execute(query)
            # Fetch the results
            results = cursor.fetchall()
            print(results)
        # If trialjob_id is not already in the dictionary, add it with an empty list
        if trialjob_id not in score_dict:
            score_dict[trialjob_id] = []

        # Append the data to the list, maintaining the order by sequence
        score_dict[trialjob_id].append(data)

    query2 = f"""
    SELECT trialjobId, data
    FROM TrialJobEvent
    WHERE event = 'WAITING'
    AND trialjobId IN (SELECT trialjobId
        FROM MetricData
        WHERE type = 'FINAL'
        GROUP BY trialjobId
        ORDER BY MAX(data) DESC
        LIMIT 5);
""" 
    cursor.execute(query2)
    results2 = cursor.fetchall()
    connection.close()
    params_dict = {}
    for row in results2:
        trialjob_id = row[0]
        parameters = json.loads(row[1])
        # If trialjob_id is not already in the dictionary, add it with an empty list
        if trialjob_id not in params_dict:
        # Append the data to the list, maintaining the order by sequence
            params_dict[trialjob_id] = (parameters['parameters'])

    merged_dict = merge_dicts(params_dict, score_dict)  
    
    if show == True:
        
        values = next(iter(merged_dict.values()))
        header = [['Trials Name'] + list(values[0].keys()) + ['Score']]

        for key, value in merged_dict.items():
            rows = [key] + list(value[0].values()) + [max(value[1])]
            header.append(rows)

        print(tabulate(header, headers='firstrow', tablefmt='grid'))    

    return merged_dict


