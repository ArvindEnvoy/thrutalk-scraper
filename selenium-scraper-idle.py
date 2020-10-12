from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import civis
import time
from bs4 import BeautifulSoup

# Civis API
client = civis.APIClient()

# Set Selenium driver options
options = webdriver.ChromeOptions()
options.add_argument('user-data-dir=Profile')
options.add_argument("--start-maximized");
options.add_argument("--no-sandbox")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--headless")

# Create an empty df
cols = ['Status', 'Email', 'Phone', 'Livevox Login', 'Service']
df = pd.DataFrame(columns=cols)

# Start chrome diver
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

# Define our ThruTalk Services
services = ['dem2020victory_co_distributed',
            'dem2020victory_co_follow_up',
            'dem2020victory_co_line_1',
            'dem2020victory_co_local_partner',
            'dem2020victory_co_outreach_for_diane',
            'dem2020victory_co_spanish_speaking',
            'dem2020victory_co']

for service in services:
    
    # Access desired URL
    url = 'https://metrics.thrutalk.io/agent-status/' + service + '_callers'
    driver.get(url)

    # Wait a sec
    time.sleep(3) # Or three

    # Get page source
    page = driver.page_source
    soup = BeautifulSoup(page, "html.parser")

    # Get table body
    table_body = soup.find_all("tbody")
    
    if len(table_body) > 0:          
        table_rows = table_body[0].find_all("tr") # Get all rows

        # Parse non-empty table body
        if len(table_rows) > 0:

            print(len(table_rows))

            table = [] # 2D list to be turned into a DataFrame
            
            for html_row in table_rows: # For each row
                
                row = [] # List for each row
                
                for cell in html_row.find_all("td"): # Get all cells
                    row.append(cell.text) # Parse cell text
                    
                table.append(row) # Add row to list of rows
        
            # Create a dataframe from the list of rows
            service_df = pd.DataFrame(table, columns=['Status', 'Email', 'Phone', 'Livevox Login'])
            service_df['Service'] = service # Add the current service as a column

            # Add to total df
            df = df.append(service_df)

# Upload to Civis
fut = civis.io.dataframe_to_civis(df=df,database="Dover",
                                  table="states_co_reporting.current_thrutalk_dialers",
                                  client=client,
                                  existing_table_rows="drop",
                                  headers=True)
print(fut.result())

driver.quit()

# Spin until pushers are done
while(True):
    vsl_job = client.jobs.get(94020798) # VSL Pusher
    toplines_job = client.jobs.get(94145346) # Toplines Pusher
    
    if (vsl_job.state != 'running') and (toplines_job != 'running'):
        break
