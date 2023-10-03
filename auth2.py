import requests

def fetch_all_comments_for_owner():
    token = input("Enter your GitHub access token: ")
    base_url = 'https://api.github.com'

    owner = input("Enter the owner of the repository: ")

    repos_url = f"{base_url}/users/{owner}/repos"

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }

    response = requests.get(repos_url, headers=headers)

    if response.status_code == 200:
        repositories_data = response.json()
        for repo_data in repositories_data:
            repo_name = repo_data['name']
            pull_requests_url = f"{base_url}/repos/{owner}/{repo_name}/pulls"

            response = requests.get(pull_requests_url, headers=headers)

            if response.status_code == 200:
                pull_requests_data = response.json()
                for pull_request in pull_requests_data:
                    pull_number = pull_request['number']
                    url = f"{base_url}/repos/{owner}/{repo_name}/pulls/{pull_number}"
                    
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        pull_request_data = response.json()
                        print(f"Repository: {owner}/{repo_name}")
                        print(f"Title: {pull_request_data['title']}")
                        print(f"Body: {pull_request_data['body']}")
                    else:
                        print(f"Failed to retrieve data for Pull Request {pull_number}. Status code: {response.status_code}")

                    comments_url = f"{base_url}/repos/{owner}/{repo_name}/issues/{pull_number}/comments"

                    response = requests.get(comments_url, headers=headers)

                    if response.status_code == 200:
                        pull_request_comments = response.json()
                        print("\nComments:")
                        for comment in pull_request_comments:
                            print(f"{comment['user']['login']}: {comment['body']}")
                    else:
                        print(f"Failed to retrieve comments for Pull Request {pull_number}. Status code: {response.status_code}")
            else:
                print(f"Failed to retrieve pull requests for {owner}/{repo_name}. Status code: {response.status_code}")
    else:
        print(f"Failed to retrieve repositories for {owner}. Status code: {response.status_code}")

fetch_all_comments_for_owner()
