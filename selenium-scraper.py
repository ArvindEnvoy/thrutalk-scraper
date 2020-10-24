import pandas as pd
import time
import datetime
import gspread as gs
import json

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
data = {
    "type": "service_account",
    "project_id": "florida2020-292120",
    "private_key_id": "0598be71c9e59b33a977669855b4c8e5680b28c4",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBiQmGDUfHge29\nysBYC58zH5VddQe7SdREYXDdDP1wnamj1WBYRJuPUAUafZbI6uowma4hAt5LaTzY\nqll5eJP9pUHba7X+t3NQnIH4GEWAILAAlAjhB5MBkDQEX5Pg7Lm74O0KgGzeJUep\nd5WYxVTRCAzTTwxwS9VqfRadsdZjXlD+s9ILEAn8VLgEMiileg6zf24T9r3saEwu\nYA94CSVJCZxvDskOngSljIWxHyXrMxHgFXqNTVgTxIabfpC+TQ02TSuX5fccgCGv\nq6QVWKKLp9Yig4QMJL4LJnTgK72LaYlOn9U7pwGVczAOrhtIbv3p8ZBiE/VTcmyy\ntey5WfY/AgMBAAECggEABgmc4vIyHY63mHalRnniVwlysu4uNhEJDrMNfs/jbip0\n+tfyv/4v7iocZqWpWQ4/DRIWRRBeV/LqA4ZY8TmqQDzyD4LONCqL7eVcBp3XOTaL\n1xDHMCH425EglkxXBCrOZoaPTocQHmQCVs6Uu6XYTgcWw1bReAVdnItXc8by/bmL\nAvsqyxbXzQEZb1yv13kPnCT4INU4mttbJp1ThciZvUatbFgiKlr0gCt7s6LNKepU\nsE01cjtf6rHKwssJiqfBkbeXtuDN9rMY0YL8F0FEufDyQuLET+ZjE8YKMqwsUHtP\npFn5IdLZ1w2Fiqb7+2ccB4zPi33KV32txcU6EILq6QKBgQD9dALYFEnDgMstPq1A\nCUcr/HiaNAQpTGCq6+192WDs6G4KBHj1yQ6rcrWfxHnaC+8+xRGkcWhS2PQEdJpG\n5FtNiVbTsVDh5VAWNNADtHI/23X2wsd4hSEY+RjSfS2iP/efEsWkgEUumacJjh2D\n8g3YtkjGnUVEurNoDo7Fw2q7+QKBgQDDeuRXp49B7m8BGoYyp3rvlhXhvFfS6Mi1\nfDyVnOOBlJT5JtZfMyNumDPCqWfHXluaLd9eOVeinXT7iUXJy8IDm4mKfaaR7Z3q\n+SkvB6PWLAKELTUKIRCrf7nepo/Lc49zMpJx3mGtro7Ioe5cEA9zoN2t3u9kPlbO\nniSLGh+h9wKBgQC7HuhuoWPoO/FFQS3lxjPOjMJ5joe3+dSwvCiFrnS508xSBwVK\n6Rq0h6cCAqu3yPHkVNh2oOfVqqlVlMTGLBgggZIiDppZfNSI4IrnAVUMQjmwahOc\nriVGa7ngxVxomnN19QUIM09gXT6OdmbiIFYKtmG6iSg32uNTotPTvDfT+QKBgQC6\nxHfWp+lf/Qdh7o25Z/s1XhiaDPF8OZ1KiOD5sWRNMkEDq++2FY4M5K7PljvpaGXs\n8VcxF2h7niVfDtD29Q7xk3HeOB8l3fm7v0NyA8Ktpm7hCrBadS0QLoDQEiPhJxAv\n6GmzEd4Rq24Qk2bB3zZkK5ahwRbImCcoUkBoBxWiEQKBgBpUljBwFKuopb3ODmBl\n6memZZ1DxeDJuoxCUT5jXj4YkWXLsLcCtyWRBDNMsXhIQrSeSLi7JXye1E3wn+Wd\nAagGuqeC1jxmznoMKkBlUCMCIfUtFaymn6Hu1Z4G8XczZivnHoMOlJB+PMDsZMm9\nAYgjZ1GlwCSztFbP5eBOhg/j\n-----END PRIVATE KEY-----\n",
    "client_email": "thrutalk-app@florida2020-292120.iam.gserviceaccount.com",
    "client_id": "106026255969120639407",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/thrutalk-app%40florida2020-292120.iam.gserviceaccount.com",
}
with open("thrutalk-test.json", "w+") as outfile:
    json.dump(data, outfile)

gc = gs.service_account(filename="thrutalk-test.json")

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
