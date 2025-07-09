import boto3
import json
import urllib3
import uuid
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MonitorBot')

def lambda_handler(event, context):
    token = "<TOKEN_GITHUB>"

    persist_info(event["service_name"], event["element"], event["message"], event["string_search"])

    count = check_for_registers(event["service_name"], event["message"])

    if count >= 3:
        title = f"[ {event["service_name"]} ] - {event["name"]}"

        description = f"""
            Issue gerada pelo serviço de monitoramento de APIs. Foi notificado o mesmo erro ocorrendo mais de três vezes em um mesmo dia.
            Serviço: {event["service_name"]}
            Problema: {event["message"]}
            Elemento da busca: {event["element"]}
            Veículo: {event["string_search"]}
            """
        create_issue_with_project(token, "InforlubeGitHub", "inforlube-infraestrutura-issue", title, description, "<ID_PROJETO_GITHUB>",["bug", "monitoramento", "bot"])


def create_issue_with_project(token, owner, repo, title, body, project_id, labels=None):
    print("Creating issue...")
    http = urllib3.PoolManager()
    issue_url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    issue_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }
    issue_data = {"title": title, "body": body, "labels": labels or [], "assignees": ["<USUARIO_GITHUB>"], "type": "Bug"}

    response = http.request(
        'POST',
        issue_url,
        headers=issue_headers,
        body=json.dumps(issue_data)
    )

    issue = json.loads(response.data.decode('utf-8'))

    print("Issue created:", issue)

    project_query = """
    mutation($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
        item { id }
      }
    }
    """

    project_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    r = http.request(
        'POST',
        "https://api.github.com/graphql",
        headers=project_headers,
        body=json.dumps({
            "query": project_query,
            "variables": {"projectId": project_id, "contentId": issue["node_id"]}
        })
    )

    print("Issue created: ", issue)

    return issue

def persist_info(service_name, element, message, string_search):
    print(f"Received data to persist: Service: {service_name}; Element: {element}; Message: {message}; Search: {string_search}")
    unique_id = uuid.uuid4()
    now = datetime.now()
    ts = datetime.timestamp(now)
    int_ts = int(ts)

    table.put_item(
        Item={
            'id': str(unique_id),
            'service': service_name,
            'element': element,
            'message': message,
            'search': string_search,
            'saved_at': int_ts
        }
    )

def check_for_registers(service, message):
    now = datetime.now()
    ts = datetime.timestamp(now)

    target_date = datetime.fromtimestamp(ts)
    start_time = target_date - timedelta(hours=12)
    end_time = target_date + timedelta(hours=12)
    
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())
    
    response = table.scan(
        FilterExpression=
        boto3.dynamodb.conditions.Attr('service').eq(service) &
        boto3.dynamodb.conditions.Attr('message').eq(message) &
        boto3.dynamodb.conditions.Attr('saved_at').between(start_timestamp, end_timestamp)
    )

    print("How many registers: ", response['Count'])

    return response['Count']
