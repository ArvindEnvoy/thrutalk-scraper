import pandas as pd
import time
import datetime
import gspread as gs

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from gspread.exceptions import WorksheetNotFound
from gspread.exceptions import SpreadsheetNotFound

# Set Selenium driver options
options = webdriver.ChromeOptions()
options.add_argument("user-data-dir=Profile")
options.add_argument("--start-maximized")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--headless")

# Start chrome diver
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

# Cookie input_relay_dial_key:"QTEyOEdDTQ.VxkL697n8UvcA6nNP8qr93NkaAuN0vP2TUu1qHsamqwT-VDXVPkqbV7Qp04.cbow7ZTzqOrSlxrf.efS922BQRGCoe1uH6fJOCI1zTYo412ZqMEuclQggw_Cg-4lAFEDwVkZxp7hmjHQ-cD3Diq4at-Fyo1MuDTOmE3SeckiJX-lPeF2_ozHkU4QvpU1jsukWzTcrRHgy6_2dt7PCgcJV-14y6eUYpybztA1MNqN4mtPxUCdpRiTx4stS6JKxsvPOawDIOVCtxQ.Dl2vZHZFjLd2GSFBLz4SMQ"
# cookie_val = "QTEyOEdDTQ.JOUlZ_8_DvpDBe7wQp54pK58VAaPxHNM9cU2KDEtn-_ug4bYwvEYcqJTcOc.DUzEs8VZeNhA5C9t.s1D4-1_rEFNF6QVoXRZzmAYuC6CIaGsqmTUj4pBsUxPJapS-vzeytVubOIP-h29ZR5fN1XnRZJNKygMYqpl-yBUk8nYAwd0d2lq0jKN_BwmmcMG4pICI-ZDwNv6WtECtjwOyIdux73G1pMp0hBkyY0cBxbc2MmN1K_779KdHJnhwFVGJAZrZIBF_BjHY92M.pD4cAIh-ZA5qK6P_2cqTvg"
dot_thrutalk_cookie = "QTEyOEdDTQ.JOUlZ_8_DvpDBe7wQp54pK58VAaPxHNM9cU2KDEtn-_ug4bYwvEYcqJTcOc.DUzEs8VZeNhA5C9t.s1D4-1_rEFNF6QVoXRZzmAYuC6CIaGsqmTUj4pBsUxPJapS-vzeytVubOIP-h29ZR5fN1XnRZJNKygMYqpl-yBUk8nYAwd0d2lq0jKN_BwmmcMG4pICI-ZDwNv6WtECtjwOyIdux73G1pMp0hBkyY0cBxbc2MmN1K_779KdHJnhwFVGJAZrZIBF_BjHY92M.pD4cAIh-ZA5qK6P_2cqTvg"

homepage = "https://thrutalk.io/admin"

driver.get(homepage)
driver.add_cookie({"name": "_relay_dial_key", "value": dot_thrutalk_cookie})
print("Thrutalk cookie set")

driver.get(
    "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_distributed_callers"
)

driver.add_cookie({"name": "_relay_dial_key", "value": dot_thrutalk_cookie})
print(".Thrutalk cookie set")
driver.get(
    "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_distributed_callers"
)

# driver.get('https://metrics.thrutalk.io/agent-status/dem2020victory_co_line_1_callers')

print(".Thrutalk cookie set")

# Define our ThruTalk Services
services = {
    "fl_dem2020victory_distributed": "Distributed",
    "fl_dem2020victory_east_central": "Pod 3 - East Central",
    "fl_dem2020victory_miamidade": "Pod 6 - Miami Dade",
    "fl_dem2020victory_north": "Pod 1 - North",
    "fl_dem2020victory_south": "Pod 4 - South",
    "fl_dem2020victory_southwest": "Pod 5 - Southwest",
    "fl_dem2020victory_spanish": "Spanish",
    "fl_dem2020victory_team": "Team",
    "fl_dem2020victory_west_central": "Pod 2 - West Central",
}


def get_agent_status_data(service):
    # url = "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_distributed_callers"
    # url = "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_southwest_callers"
    # url = "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_south"
    # url = "https://metrics.thrutalk.io/agent-status/fl_dem2020victory_west_central_callers"

    url = "https://metrics.thrutalk.io/agent-status/" + service + "_callers"

    driver.get(url)
    driver.add_cookie({"name": "_relay_dial_key", "value": dot_thrutalk_cookie})

    # Wait for some time
    time.sleep(10)

    # Get page source
    page = driver.page_source
    soup = BeautifulSoup(page, "html.parser")

    # Get table body
    table_body = soup.find_all("tbody")
    # import pdb

    # pdb.set_trace()
    if len(table_body) > 0:
        table_rows = table_body[0].find_all("tr")  # Get all rows

        # Parse non-empty table body
        if len(table_rows) > 0:
            table = []
            for html_row in table_rows:  # For each row
                row = []  # List for each row
                for cell in html_row.find_all("td"):  # Get all cells
                    row.append(cell.text)  # Parse cell text
                table.append(row)  # Add row to list of rows
            service_df = pd.DataFrame(  # Create a dataframe from the list of rows
                table, columns=["Status", "Email", "Phone", "Livevox Login"]
            )
            service_df["Service"] = service
        else:
            service_df = pd.DataFrame(
                columns=["Status", "Email", "Phone", "Livevox Login"]
            )
            service_df["Service"] = service
        return service_df
    else:
        return pd.DataFrame()


def update_worksheet(ws, df):

    current_time = datetime.datetime.now().replace(microsecond=0) + datetime.timedelta(
        hours=-4
    )
    columns = df.columns.to_list()
    bookmark = {}

    for idx, val in enumerate(columns):
        if idx == 1:
            bookmark[val] = "Time run: "
        elif idx == 2:
            bookmark[val] = str(current_time.strftime("%I:%M%p")) + " EST"
        else:
            bookmark[val] = ""

    new_data = (
        pd.DataFrame(ws.get_all_records())
        .append(pd.Series(), ignore_index=True)
        .append(bookmark, ignore_index=True)
        .fillna("")
        .append(df)
    )

    ws.update([new_data.columns.values.tolist()] + new_data.fillna("").values.tolist())


# Create an empty df, this is where we will populate data from the agent logs
cols = ["Status", "Email", "Phone", "Livevox Login", "Service"]
df = pd.DataFrame(columns=cols)
gc = gs.service_account(filename="thrutalk-creds.json")

for service in services.keys():
    service_df = get_agent_status_data(service)

    current_date = str((datetime.datetime.now() + datetime.timedelta(hours=-4)).date())
    try:
        sh = gc.open(services[service])
    except SpreadsheetNotFound:
        print(f"{service} spreadsheet not found")

    try:
        ws = sh.worksheet(current_date)
    except WorksheetNotFound:
        ws = sh.add_worksheet(current_date, rows=100, cols=20)
    update_worksheet(ws, service_df)

    # ws.share("arvinddd2003@gmail.com", perm_type="user", role="writer")
driver.quit()
