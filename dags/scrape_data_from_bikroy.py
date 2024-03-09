from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import os
from datetime import datetime
from database import upload_to_mongo

path = 'c://chromedriver.exe'
browser = webdriver.Chrome()
url = "https://bikroy.com/en"
browser.get(url=url)
jobs_category_link = browser.find_element(By.XPATH, '//a[@href="/en/jobs"]')
jobs_category_link.click()

chefs_subcategory = browser.find_element(By.XPATH, '//a[@href="/en/ads/bangladesh/chef-jobs"]')
chefs_subcategory.click()

wait = WebDriverWait(browser, 20)
job_title_elements = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, '.heading--2eONR.heading-2--1OnX8.title--3yncE.block--3v-Ow')))
images = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'img.normal-ad--1TyjD')))

data = []
for job_title_element,img in zip(job_title_elements,images):
    try:
        
        parent_div = job_title_element.find_element(By.XPATH, './ancestor::div[@class="content--3JNQz"]')
        child_div = parent_div.find_element(By.XPATH, './/div')
        child_data = child_div.text
        child_components = child_data.split('\n')
        child_string = ' | '.join(child_components)
        job_title = job_title_element.text  # Extracting text from job_title_element
        image_url = img.get_attribute('src')
        data.append({
            "job_title": job_title,  # Using extracted text
            "image_url": image_url, #base64
            "details": child_string,
            "scraped_datetime": datetime.now()
        })
        # print(job_title) 
        # Print the extracted data
        # print(job_title)
        # print(child_data)
        # print(image_url)
        
    except NoSuchElementException:
        print("Parent div not found for job title element:", job_title_element.text)


df = pd.DataFrame(data)
data_dir = os.path.join(os.getcwd(), 'data')
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
file_name = 'bikroy_jobs.csv'
csv_file = os.path.join(data_dir,file_name)
df.to_csv(csv_file, index=False)

# insert data to mongodb
upload_to_mongo(file_name=file_name,collection_name='bikroy_jobs')
# print(df)
# Close the browser
browser.quit()