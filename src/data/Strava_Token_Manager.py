from pathlib import Path
import requests
import yaml
import time
from datetime import datetime, timedelta
from typing import Optional, Dict


class StravaTokenManager:
    """Manages multiple Strava accounts with fixed 15-min rate limit windows"""
    
    # Strava limits: 100 calls per 15min, 1000 calls per day
    CALLS_PER_15MIN = 100
    CALLS_PER_DAY = 1000
    WINDOW_MINUTES = 15
    
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config()
        self.accounts = self._load_accounts()
        self.current_account_idx = 0
        
        # Track usage per account
        self.account_stats = {}
        for account in self.accounts:
            name = account['name']
            self.account_stats[name] = {
                'calls_this_window': 0,
                'calls_today': 0,
                'current_window_start': self._get_current_window_start(),
                'day_start': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            }
    
    def _get_current_window_start(self) -> datetime:
        """Get the start time of current 15-min window (e.g., 20:00, 20:15, 20:30)"""
        now = datetime.now()
        minutes_into_hour = now.minute
        window_number = minutes_into_hour // self.WINDOW_MINUTES
        window_start_minute = window_number * self.WINDOW_MINUTES
        
        return now.replace(minute=window_start_minute, second=0, microsecond=0)
    
    def _get_next_window_start(self) -> datetime:
        """Get when the next 15-min window starts"""
        current = self._get_current_window_start()
        return current + timedelta(minutes=self.WINDOW_MINUTES)
    
    def _reset_window_if_needed(self, account_name: str):
        """Reset call counter if we're in a new 15-min window"""
        stats = self.account_stats[account_name]
        current_window = self._get_current_window_start()
        
        # Check if window changed
        if current_window > stats['current_window_start']:
            old_calls = stats['calls_this_window']
            stats['calls_this_window'] = 0
            stats['current_window_start'] = current_window
            print(f"🔄 {account_name}: New window started (previous: {old_calls}/100 calls)")
        
        # Check if day changed
        current_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if current_day > stats['day_start']:
            stats['calls_today'] = 0
            stats['day_start'] = current_day
            print(f"📅 {account_name}: New day started - daily limit reset")
    
    def _load_config(self) -> dict:
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _load_accounts(self) -> list:
        """Extract all Strava accounts from config"""
        accounts = []
        
        # Primary account
        if 'strava' in self.config:
            accounts.append({
                'name': 'strava',
                'config': self.config['strava']
            })
        
        # Secondary accounts (strava2, strava3, etc.)
        for key in sorted(self.config.keys()):
            if key.startswith('strava') and key != 'strava':
                accounts.append({
                    'name': key,
                    'config': self.config[key]
                })
        
        print(f"✓ Loaded {len(accounts)} Strava account(s): {[a['name'] for a in accounts]}")
        return accounts
    
    def _save_config(self):
        """Save updated tokens to config file"""
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
    
    def _refresh_token(self, account_name: str) -> Optional[str]:
        """Refresh access token for a specific account"""
        account = next(a for a in self.accounts if a['name'] == account_name)
        strava_config = account['config']
        
        print(f"🔄 Refreshing token for {account_name}...")
        
        response = requests.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": strava_config['client_id'],
                "client_secret": strava_config['client_secret'],
                "grant_type": "refresh_token",
                "refresh_token": strava_config['refresh_token']
            }
        )
        
        if response.status_code == 200:
            tokens = response.json()
            self.config[account_name]['access_token'] = tokens['access_token']
            self.config[account_name]['refresh_token'] = tokens['refresh_token']
            self._save_config()
            
            # Update in-memory account config
            account['config']['access_token'] = tokens['access_token']
            account['config']['refresh_token'] = tokens['refresh_token']
            
            print(f"✓ Token refreshed for {account_name}")
            return tokens['access_token']
        else:
            print(f"✗ Token refresh failed for {account_name}: {response.text}")
            return None
    
    def _validate_token(self, access_token: str) -> bool:
        """Check if token is valid"""
        response = requests.get(
            "https://www.strava.com/api/v3/athlete",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        return response.status_code == 200
    
    def get_current_account(self) -> Dict:
        """Get current active account"""
        return self.accounts[self.current_account_idx]
    
    def can_make_call(self, account_name: str) -> bool:
        """Check if account can make another API call"""
        self._reset_window_if_needed(account_name)
        stats = self.account_stats[account_name]
        
        return (stats['calls_this_window'] < self.CALLS_PER_15MIN and 
                stats['calls_today'] < self.CALLS_PER_DAY)
    
    def record_api_call(self, account_name: str):
        """Increment call counters for an account"""
        self._reset_window_if_needed(account_name)
        stats = self.account_stats[account_name]
        stats['calls_this_window'] += 1
        stats['calls_today'] += 1
    
    def get_valid_token(self) -> str:
        """Get a valid access token from current account"""
        account = self.get_current_account()
        account_name = account['name']
        access_token = account['config']['access_token']
        
        # Check if token is valid
        if self._validate_token(access_token):
            return access_token
        
        # Token expired, refresh it
        print(f"Token expired for {account_name}, refreshing...")
        return self._refresh_token(account_name)
    
    def switch_account(self) -> bool:
        """Switch to next available account that can make calls"""
        # Try each account (excluding current)
        for offset in range(1, len(self.accounts)):
            next_idx = (self.current_account_idx + offset) % len(self.accounts)
            account_name = self.accounts[next_idx]['name']
            
            if self.can_make_call(account_name):
                self.current_account_idx = next_idx
                stats = self.account_stats[account_name]
                print(f"🔄 Switched to {account_name} (used: {stats['calls_this_window']}/100 this window, {stats['calls_today']}/1000 today)")
                return True
        
        # No accounts available
        return False
    
    def get_headers(self) -> dict:
        """Get authorization headers for current account"""
        token = self.get_valid_token()
        return {"Authorization": f"Bearer {token}"}
    
    def wait_for_next_window(self):
        """Wait until the next 15-min window starts"""
        next_window = self._get_next_window_start()
        now = datetime.now()
        wait_seconds = (next_window - now).total_seconds()
        
        if wait_seconds > 0:
            print(f"\n⏰ Waiting {int(wait_seconds)}s until next window ({next_window.strftime('%H:%M:%S')})")
            time.sleep(wait_seconds + 1)  # +1s buffer
    
    def handle_rate_limit(self) -> bool:
        """
        Handle rate limit by switching accounts or waiting.
        Returns True if can continue, False if should stop.
        """
        current_account = self.get_current_account()['name']
        
        print("\n" + "="*60)
        print("⚠️  RATE LIMIT HIT")
        print("="*60)
        
        # Record that we hit the limit
        stats = self.account_stats[current_account]
        print(f"📊 {current_account}: {stats['calls_this_window']}/100 (window), {stats['calls_today']}/1000 (day)")
        
        # Try to switch to another account
        if self.switch_account():
            print("✓ Continuing with new account")
            print("="*60 + "\n")
            return True
        
        # All accounts exhausted - check if we should wait
        print("⚠️  All accounts exhausted in this window")
        
        # Check if ANY account can recover (not at daily limit)
        any_can_recover = any(
            stats['calls_today'] < self.CALLS_PER_DAY 
            for stats in self.account_stats.values()
        )
        
        if any_can_recover:
            print("💡 Waiting for next 15-min window...")
            self.wait_for_next_window()
            
            # Try switching again after window reset
            if self.switch_account():
                print("✓ Resumed after window reset")
                print("="*60 + "\n")
                return True
        else:
            print("❌ All accounts hit daily limit (1000 calls)")
            print("="*60 + "\n")
            return False
        
        print("="*60 + "\n")
        return False
    
    def print_status(self):
        """Print current status of all accounts"""
        print(f"\n📊 API Usage Status (Window: {self._get_current_window_start().strftime('%H:%M')})")
        print("-" * 60)
        for account in self.accounts:
            name = account['name']
            stats = self.account_stats[name]
            current = "← ACTIVE" if name == self.get_current_account()['name'] else ""
            
            window_pct = (stats['calls_this_window'] / self.CALLS_PER_15MIN) * 100
            day_pct = (stats['calls_today'] / self.CALLS_PER_DAY) * 100
            
            print(f"{name:12} | Window: {stats['calls_this_window']:3}/100 ({window_pct:5.1f}%) | "
                  f"Day: {stats['calls_today']:4}/1000 ({day_pct:5.1f}%) {current}")
        print("-" * 60)


def make_strava_request_with_retry(token_manager: StravaTokenManager, 
                                   url: str, 
                                   params: dict = None,
                                   max_retries: int = 10) -> Optional[requests.Response]:
    """
    Make a Strava API request with automatic account switching.
    """
    for attempt in range(max_retries):
        current_account = token_manager.get_current_account()['name']
        
        # Check if current account can make calls
        if not token_manager.can_make_call(current_account):
            if not token_manager.handle_rate_limit():
                return None
            continue
        
        # Make the request
        headers = token_manager.get_headers()
        response = requests.get(url, headers=headers, params=params)
        
        # Record the call (even if it fails)
        token_manager.record_api_call(current_account)
        
        if response.status_code == 200:
            return response
        
        elif response.status_code == 429:
            # Rate limit hit (shouldn't happen if we track correctly, but just in case)
            if not token_manager.handle_rate_limit():
                return None
            continue
        
        elif response.status_code == 401:
            # Token expired
            print("🔄 Token expired, refreshing...")
            token_manager.get_valid_token()
            continue
        
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
            return None
    
    print(f"❌ Max retries ({max_retries}) reached")
    return None