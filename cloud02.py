import logging
import os
import requests
import re
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the retry session function
def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Define the email extraction function
def extract_email(text):
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(pattern, text)
    if matches:
        return matches[0].strip('\"<>[]()')
    return None

# Define the GitHub API handler class
class GitHubApiHandler:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.request_count = 0
        self.max_requests_per_key = 3650
        self.failed_attempts = 0

    def get_headers(self):
        return {'Authorization': f'token {self.api_keys[self.current_key_index]}'}

    def check_and_switch_key(self):
        remaining_requests = self.get_remaining_requests()
        logger.info(f"Remaining requests for current key: {remaining_requests}")
        if remaining_requests < 10:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            self.request_count = 0
            self.failed_attempts += 1
            logger.info(f"Switched to new API key: {self.current_key_index + 1}")
            if self.failed_attempts >= 18:
                logger.info("API rate limit hit for all keys. Waiting for 1 hour and 5 minutes.")
                time.sleep(3900)  # Wait for 1 hour and 5 minutes
                self.failed_attempts = 0

    def get_remaining_requests(self):
        headers = self.get_headers()
        url = 'https://api.github.com/rate_limit'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code == 200:
            rate_limit_data = response.json()
            remaining = rate_limit_data['rate']['remaining']
            return remaining
        return 0

    def get_user_info_from_github_api(self, username_or_url):
        self.check_and_switch_key()
        headers = self.get_headers()
        self.request_count += 1
        if username_or_url.startswith('https://github.com/'):
            username = username_or_url.split('/')[-1]
        else:
            username = username_or_url
        url = f'https://api.github.com/users/{username}'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code != 200:
            logger.info(f"Failed to fetch user info for {username_or_url}, status code: {response.status_code}")
            return None
        user_data = response.json()
        email = user_data.get('email', '') or self.get_email_from_readme(username, headers)
        return email

    def get_email_from_readme(self, username, headers):
        url = f'https://raw.githubusercontent.com/{username}/{username}/main/README.md'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return extract_email(response.text)
        return None

# Define the main function
def main():
    try:
        input_csv_path = 'input2.csv'
        output_csv_path = 'output2.csv'
        last_save_time = datetime.now()
        
        api_keys = os.environ['MY_GITHUB_API_KEYS'].split(',')
        
        github_api_handler = GitHubApiHandler(api_keys)
        
        logger.info("Reading input CSV file...")
        input_df = pd.read_csv(input_csv_path)
        
        if os.path.exists(output_csv_path):
            output_df = pd.read_csv(output_csv_path)
            processed_profiles = set(output_df['Profile URL'])
        else:
            output_df = pd.DataFrame(columns=['Username', 'User ID', 'Profile URL', 'Email'])
            processed_profiles = set()

        new_rows = []
        for index, row in input_df.iterrows():
            profile_url = row['Profile URL']
            if row.get('Status') == 'Done' or profile_url in processed_profiles:
                continue

            username = row['Username']
            user_id = row['User ID']
            logger.info(f"Processing {username} ({profile_url})")

            try:
                email = github_api_handler.get_user_info_from_github_api(profile_url)
                if email:
                    new_rows.append({
                        'Username': username,
                        'User ID': user_id,
                        'Profile URL': profile_url,
                        'Email': email
                    })
                    processed_profiles.add(profile_url)
                    input_df.at[index, 'Status'] = 'Done'  # Mark as done

                # Save progress every 30 minutes
                if (datetime.now() - last_save_time) > timedelta(minutes=30):
                    logger.info("Saving progress...")
                    if new_rows:
                        new_df = pd.DataFrame(new_rows)
                        output_df = pd.concat([output_df, new_df], ignore_index=True)
                        output_df.to_csv(output_csv_path, index=False)
                        new_rows.clear()  # Clear the list after saving
                    input_df.to_csv(input_csv_path, index=False)
                    last_save_time = datetime.now()
                    
            except Exception as e:
                logger.error(f"An error occurred while processing {profile_url}: {e}")

        # Final save after loop
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            output_df = pd.concat([output_df, new_df], ignore_index=True)
            output_df.to_csv(output_csv_path, index=False)
        input_df.to_csv(input_csv_path, index=False)

        logger.info(f"Successfully written output to {output_csv_path}")

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
