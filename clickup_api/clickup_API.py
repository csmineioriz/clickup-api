import requests
import json
import os
import time
import pandas as pd

# Caminhos de arquivos
ALL_DATA_FILE = r"C:\\Temp\\clickup_all_data.json"
TEMP_DIR = r"C:\\Temp"

# Carregar token
with open(r"C:\\Users\\ciro.minei\\OneDrive - ORIZ PARTNERS\\_python\\config.json") as config_file:
    config = json.load(config_file)
    API_TOKEN = config["clickup_api_token"]

HEADERS = {
    'Authorization': API_TOKEN,
    'Content-Type': 'application/json'
}

# URLs base
TEAM_URL = "https://api.clickup.com/api/v2/team"
SPACES_IN_TEAM_URL = "https://api.clickup.com/api/v2/team/{team_id}/space"
LISTS_IN_SPACE_URL = "https://api.clickup.com/api/v2/space/{space_id}/list"
FIELDS_IN_LIST_URL = "https://api.clickup.com/api/v2/list/{list_id}/field"
GROUP_URL = "https://api.clickup.com/api/v2/group"
TASKS_IN_LIST_URL = "https://api.clickup.com/api/v2/list/{list_id}/task"
TASK_URL = "https://api.clickup.com/api/v2/task/{task_id}"
CFIELD_IN_TASK_BY_ID_URL = "https://api.clickup.com/api/v2/task/{task_id}/field/{field_id}"

# Função para apagar arquivos temporários
def delete_temp_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

# Função para consolidar todos os dados em um arquivo JSON, incluindo grupos
def fetch_all():
    delete_temp_file(ALL_DATA_FILE)
    """Busca e consolida todos os dados em um arquivo JSON."""
    all_data = {}

    # Obter teams
    response = requests.get(TEAM_URL, headers=HEADERS)
    if response.status_code == 200:
        teams = response.json().get("teams", [])
        all_data["teams"] = []
        for team in teams:
            team_data = {
                "id": team["id"],
                "name": team["name"],
                "spaces": [],
                "groups": []
            }

            # Obter grupos para cada team
            query = {"team_id": team["id"]}
            groups_response = requests.get(GROUP_URL, headers=HEADERS, params=query)
            if groups_response.status_code == 200:
                groups = groups_response.json().get("groups", [])
                for group in groups:
                    team_data["groups"].append({
                        "id": group["id"],
                        "name": group["name"]
                    })
            else:
                print(f"Erro ao buscar grupos: {groups_response.status_code}")

            # Obter spaces para cada team
            spaces_url = SPACES_IN_TEAM_URL.format(team_id=team["id"])
            spaces_response = requests.get(spaces_url, headers=HEADERS)
            if spaces_response.status_code == 200:
                spaces = spaces_response.json().get("spaces", [])
                for space in spaces:
                    space_data = {
                        "id": space["id"],
                        "name": space["name"],
                        "lists": []
                    }

                    # Obter listas para cada space
                    lists_url = LISTS_IN_SPACE_URL.format(space_id=space["id"])
                    lists_response = requests.get(lists_url, headers=HEADERS)
                    if lists_response.status_code == 200:
                        lists = lists_response.json().get("lists", [])
                        for lst in lists:
                            list_data = {
                                "id": lst["id"],
                                "name": lst["name"],
                                "custom_fields": []
                            }

                            # Obter custom fields para cada lista
                            fields_url = FIELDS_IN_LIST_URL.format(list_id=lst["id"])
                            fields_response = requests.get(fields_url, headers=HEADERS)
                            if fields_response.status_code == 200:
                                fields = fields_response.json().get("fields", [])
                                for field in fields:
                                    list_data["custom_fields"].append({
                                        "id": field["id"],
                                        "name": field["name"],
                                        "type_config": field["type_config"],
                                        "date_created": field["date_created"],
                                        "hide_from_guests": field["hide_from_guests"],
                                        "required": field["required"]
                                    })

                            space_data["lists"].append(list_data)

                    team_data["spaces"].append(space_data)

            all_data["teams"].append(team_data)
    else:
        print(f"Erro ao buscar teams: {response.status_code}")

    # Salvar em arquivo JSON
    with open(ALL_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    print("Dados consolidados salvos em clickup_all_data.json")

# Funções para buscar IDs a partir do JSON consolidado
def load_all_data():
    if os.path.exists(ALL_DATA_FILE):
        with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print("Arquivo consolidado não encontrado. Execute fetch_all primeiro.")
        return None

def get_team_id_by_name(team_name):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            if team["name"].lower() == team_name.lower():
                return team["id"]
    return None

def get_space_id_by_name(team_name, space_name):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            if team["name"].lower() == team_name.lower():
                for space in team.get("spaces", []):
                    if space["name"].lower() == space_name.lower():
                        return space["id"]
    return None

def get_list_id_by_name(team_name, space_name, list_name):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            if team["name"].lower() == team_name.lower():
                for space in team.get("spaces", []):
                    if space["name"].lower() == space_name.lower():
                        for lst in space.get("lists", []):
                            if lst["name"].lower() == list_name.lower():
                                return lst["id"]
    return None

def get_custom_field_id_by_name(scope_name, field_name, scope_type="list"):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            for space in team.get("spaces", []):
                for lst in space.get("lists", []):
                    if scope_type == "list" and lst["name"].lower() == scope_name.lower():
                        for field in lst.get("custom_fields", []):
                            if field["name"].lower() == field_name.lower():
                                return field["id"]
    return None

def get_custom_field_options_by_custom_field_name(scope_name, field_name, scope_type="list"):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            for space in team.get("spaces", []):
                for lst in space.get("lists", []):
                    if scope_type == "list" and lst["name"].lower() == scope_name.lower():
                        for field in lst.get("custom_fields", []):
                            if field["name"].lower() == field_name.lower():
                                return field.get("type_config", {}).get("options", [])
    return None

def get_group_id_by_name(team_name, group_name):
    data = load_all_data()
    if data:
        for team in data.get("teams", []):
            if team["name"].lower() == team_name.lower():
                for group in team.get("groups", []):
                    if group["name"].lower() == group_name.lower():
                        return group["id"]
    return None


# Função para buscar todas as tasks e transformar em DataFrame
def get_tasks_dataframe(list_id):
    tasks = []
    page = 0
    limit = 100
    has_more_tasks = True

    while has_more_tasks:
        url = TASKS_IN_LIST_URL.format(list_id=list_id)
        params = {'subtasks': True, 'include_closed': 'true', 'page': page, 'limit': limit}
        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 200:
            tasks_data = response.json()
            tasks.extend(tasks_data.get('tasks', []))
            if len(tasks_data['tasks']) < limit:
                has_more_tasks = False
            else:
                page += 1
        elif response.status_code == 429:
            print("Limite de requisições atingido. Aguardando 0,5 segundos...")
            time.sleep(0.5)
        else:
            print(f"Erro ao buscar tasks da lista {list_id}: Código {response.status_code}, Detalhes: {response.text}")
            has_more_tasks = False

    # Criar o DataFrame e adicionar colunas auxiliares
    df = pd.DataFrame(tasks)
    
    # Criando um dicionário de referência de ID para nome
    id_to_name = df.set_index('id')['name'].to_dict()
    
    # Criando a coluna 'parent_name' com base no mapeamento do dicionário
    df['parent_name'] = df['parent'].map(id_to_name)
    
    return df


# Atualizar função de status com print
def update_task_status(task_id, payload, task_name, parent_name=None):
    url = TASK_URL.format(task_id=task_id)
    # payload = {'status': novo_status}
    response = requests.put(url, json=payload, headers=HEADERS)
    
    if response.status_code == 200:
        if parent_name is not None:
            message = f"Subtask '{task_name}' (ID: {task_id}) da task '{parent_name}' atualizada: '{payload}'."
        else:
            message = f"Task '{task_name}' (ID: {task_id}) atualizada: {payload}."
        print(message)
    elif response.status_code == 429:
        print("Limite de requisições atingido. Aguardando 0,5 segundos...")
        time.sleep(0.5)
        return update_task_status(task_id, payload, task_name, parent_name)
    else:
        print(f"Erro ao atualizar o status da tarefa {task_id}: Código {response.status_code}, Detalhes: {response.text}")


# Função para criar uma subtarefa
def create_subtask(list_id, payload):
    url = TASKS_IN_LIST_URL.format(list_id=list_id)
    
    try:
        response = requests.post(url, json=payload, headers=HEADERS)
        if response.status_code == 200:
            subtarefa_data = response.json()
            print(f"Subtarefa criada e atribuída com sucesso: {subtarefa_data['id']}")
            return subtarefa_data['id']
        elif response.status_code == 429:
            print("Limite de requisições atingido. Aguardando 0,5 segundos...")
            time.sleep(0.5)
            # Tenta novamente
            return create_subtask(list_id, payload)
        else:
            print(f"Erro ao criar subtarefa {payload['name']}: Código {response.status_code}, Detalhes: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao criar a subtarefa {payload['name']}: {e}")
        return None
    
# Atualizar função de status com print
def update_custom_field(task_id, field_id, payload):
    # usual payload should be: payload = {'value': new_value}
    url = CFIELD_IN_TASK_BY_ID_URL.format(task_id=task_id, field_id=field_id)
    
    response = requests.post(url, json=payload, headers=HEADERS)
    
    if response.status_code == 200:
        print(f"Custom field ID: {field_id} da task ID: {task_id} atualizada: '{payload}'.")
    elif response.status_code == 429:
        print("Limite de requisições atingido. Aguardando 0,5 segundos...")
        time.sleep(0.5)
        return update_custom_field(task_id, field_id, payload)
    else:
        print(f"Erro ao atualizar o custom field {field_id}: Código {response.status_code}, Detalhes: {response.text}")



def get_custom_field_option(custom_fields, custom_field_name):
    """
    Retorna a opção de um campo personalizado com base no valor correspondente definido para o campo.

    :param custom_fields: Lista de dicionários representando os campos personalizados.
    :param custom_field_name: Nome do campo personalizado que deve ser processado.
    :return: Nome da opção correspondente ao valor do campo ou None se não encontrado.
    """
    # Procurar o campo correspondente
    custom_field = next((field for field in custom_fields if field['name'] == custom_field_name), None)

    # Verificar se o campo foi encontrado e possui opções
    if custom_field and 'type_config' in custom_field and 'options' in custom_field['type_config']:
        field_value = custom_field.get('value')
        
        # Procurar a opção correspondente ao valor do campo
        option_name = next(
            (option['name'] for option in custom_field['type_config']['options'] if option['orderindex'] == field_value),
            None
        )
        return option_name

    # Retornar None se o campo ou a opção correspondente não for encontrada
    return None

# # Exemplo de uso
# custom_fields_example = [
#     {
#         "name": "Status Solução Societario",
#         "type_config": {
#             "options": [
#                 {"name": "OK", "orderindex": 1},
#                 {"name": "NOK", "orderindex": 2},
#                 {"name": "Pendente", "orderindex": 3}
#             ]
#         },
#         "value": 2
#     }
# ]

# result = get_custom_field_option(custom_fields_example, "Status Solução Societario")
# print(result)  # Saída: NOK


def get_custom_field_value(custom_fields, custom_field_name):
    """
    Retorna o valor de um campo personalizado.

    :param custom_fields: Lista de dicionários representando os campos personalizados.
    :param custom_field_name: Nome do campo personalizado que deve ser processado.
    :return: Valor do campo ou None se não encontrado.
    """
    # Procurar o campo correspondente
    field_value = next((field.get('value') for field in custom_fields if field['name'] == custom_field_name), None)
    return field_value

# *** testar posteriormente ***
# Função para obter os IDs e nomes dos usuários do time
def get_team_members():
    response = requests.get(TEAM_URL, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if 'teams' in data and len(data['teams']) > 0 and 'members' in data['teams'][0]:
            # Remover espaços dos nomes para normalizar
            return {user['user']['username'].strip(): user['user']['id'] for user in data['teams'][0]['members']}
        else:
            raise Exception("Estrutura da resposta inesperada: 'teams' ou 'members' ausente.")
    else:
        raise Exception("Erro ao obter membros do time: ", response.status_code)
    
    
def update_clickup_custom_fields_batch(df, update_plan):
    """
    Atualiza múltiplos campos customizados no ClickUp com base em um DataFrame.

    Parâmetros:
        df (pd.DataFrame): DataFrame com coluna 'id' e os dados a atualizar
        update_plan (list of dict): Lista com instruções de update.
            Cada dict deve conter:
                - 'cfield_id': ID do campo customizado no ClickUp
                - 'cfield_name': Nome da coluna no DataFrame com o valor
                - 'include_time' (opcional): True se valor for datetime

    Exemplo de update_plan:
        [
            {'cfield_id': 'abc123', 'cfield_name': 'CriadoEm_unix', 'include_time': True},
            {'cfield_id': 'def456', 'cfield_name': 'DataDeEntrega_unix', 'include_time': False}
        ]
    """
    for update_cfg in update_plan:
        cfield_id = update_cfg['cfield_id']
        cfield_name = update_cfg['cfield_name']
        include_time = update_cfg.get('include_time', False)

        print(f"\n▶ Atualizando campo '{cfield_name}' com ID '{cfield_id}'...")

        df.apply(
            lambda row: _update_cfield(row, cfield_id, cfield_name, include_time),
            axis=1
        )

def _update_cfield(row, cfield_id, cfield_name, include_time=True):
    payload = {'value': row[cfield_name]}
    if include_time:
        payload['value_options'] = {"time": True}

    return update_custom_field(row['id'], cfield_id, payload)
