import requests
import yaml
from pathlib import Path

def refresh_strava_token(config_path: Path) -> str:
    """
    Refresh Strava access token using refresh_token.
    Updates config.yaml with new tokens.
    
    Your config.yaml should have:
    strava:
      client_id: "YOUR_CLIENT_ID"
      client_secret: "YOUR_CLIENT_SECRET"
      access_token: "YOUR_ACCESS_TOKEN"
      refresh_token: "YOUR_REFRESH_TOKEN"
    """
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    strava = config['strava']
    
    # Request new tokens
    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": strava['client_id'],
            "client_secret": strava['client_secret'],
            "grant_type": "refresh_token",
            "refresh_token": strava['refresh_token']
        }
    )
    
    if response.status_code == 200:
        tokens = response.json()
        
        # Update config with new tokens
        config['strava']['access_token'] = tokens['access_token']
        config['strava']['refresh_token'] = tokens['refresh_token']
        
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        print("✓ Token refreshed successfully!")
        return tokens['access_token']
    else:
        print(f"✗ Token refresh failed: {response.text}")
        return None


def get_valid_token(config_path: Path) -> str:
    """Get a valid access token, refreshing if needed."""
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    access_token = config['strava']['access_token']
    
    # Test if token is valid
    response = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    
    if response.status_code == 200:
        print("✓ Token is valid")
        return access_token
    elif response.status_code == 401:
        print("Token expired, refreshing...")
        return refresh_strava_token(config_path)
    else:
        print(f"✗ Unknown error: {response.status_code}")
        return None


# USAGE in your main script:
if __name__ == "__main__":
    config_path = "../config.yaml"
    token = get_valid_token(config_path)
    print(f"Access token: {token[:20]}...")