# GlassyReply

This bot connects to a Gmail account, monitors the inbox for new emails, and uses the Google Gemini API to generate draft replies. You can interact with your emails directly from a private Telegram chat, allowing you to send replies, save drafts, apply labels, forward messages, and more.

## Features

- **Automatic Email Fetching**: Monitors your Gmail inbox and sends new emails to a Telegram chat.
- **AI-Powered Replies**: Uses Google Gemini (you can choose which model you prefer) to automatically generate a draft reply for each new email.
- **Interactive Controls**: Manage emails using Telegram's inline keyboard buttons:
  - Send, Save as Draft, Trash, or Reject AI suggestion.
  - Star or Unstar messages.
  - Apply Gmail labels.
  - Forward emails to predefined or custom addresses.
  - Download attachments.
- **Customizable AI Prompts**: Reply to the bot's message to provide a custom prompt for the AI.
- **Secure**: Uses OAuth2 for Gmail authentication and stores credentials locally.

---

## Prerequisites

- Python 3.8+
- A Google Account
- A Telegram Account

---

## Setup Instructions

Follow these steps carefully to configure and run the bot.

### 1. Clone the Project

First, get the code onto your local machine. If you're using Git, you can clone it. Otherwise, download the source files.

### 2. Install Dependencies

This project requires several Python libraries. A `requirements.txt` file is included to simplify installation. Open your terminal in the project directory and run:

```bash
pip install -r requirements.txt
```

### 3. Configure Google Cloud & Gmail API

The bot needs API access to read and send emails.

1.  **Go to the Google Cloud Console**: [console.cloud.google.com](https://console.cloud.google.com/)
2.  **Create a new project**: If you don't have one already, create a new project (e.g., "Telegram Bot").
3.  **Enable the Gmail API**:
    - In the search bar, type "Gmail API" and select it.
    - Click the **Enable** button.
4.  **Create Credentials**:
    - Go to the **Credentials** page from the left-hand menu.
    - Click **+ CREATE CREDENTIALS** and select **OAuth client ID**.
    - If prompted, configure the **OAuth consent screen**.
        - Select **External** and click **Create**.
        - App name: `Gmail AI Bot` (or anything you like).
        - User support email: Your email address.
        - Developer contact information: Your email address.
        - Click **Save and Continue** through the Scopes and Test Users sections. You do not need to add test users for this to work.
    - Now, back on the Credentials page, create the OAuth client ID:
        - Application type: **Desktop app**.
        - Name: `Gmail Bot Credentials` (or anything you like).
        - Click **Create**.
5.  **Download Credentials File**:
    - A popup will show your Client ID and Secret. Click **DOWNLOAD JSON**.
    - **Rename the downloaded file to `credentials.json`** and place it in the root of the project directory. This file is essential for authentication.

### 4. Get a Google Gemini API Key

The bot uses the Gemini API for AI-generated replies.

1.  Go to **Google AI Studio**: [aistudio.google.com](https://aistudio.google.com/)
2.  Click **Get API key** and then **Create API key in new project**.
3.  Copy the generated API key. You will need it in the next step.

### 5. Configure the Telegram Bot

1.  **Create a new bot with BotFather**:
    - Open Telegram and search for the `@BotFather` user.
    - Start a chat and send the `/newbot` command.
    - Follow the prompts to set a name and username for your bot.
    - BotFather will give you a **token**. Copy it.
2.  **Get your Telegram Chat ID**:
    - The bot is designed to work in a private chat with you. You need your unique Chat ID.
    - Search for the bot `@userinfobot` on Telegram and start a chat.
    - It will immediately send you a message containing your user information, including your **ID**. Copy it.

### 6. Create the Environment File

The bot uses a `.env` file to store your secret keys and configuration.

1.  Create a file named `.env` in the project's root directory.
2.  Copy the following content into it, replacing the placeholder values with your actual credentials from the steps above.

```env
TELEGRAM_BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID="YOUR_TELEGRAM_CHAT_ID_HERE"
GOOGLE_API_KEY="YOUR_GEMINI_API_KEY_HERE"
ENABLE_PIXEL=true/false
PIXEL_BASE_URL=your_cloudfare_worker_url
```

---

## Running the Bot

### First Run & Authentication

The first time you run the bot, it will need to authenticate with your Google Account.

1.  Open a terminal in the project directory and run the script:
    ```bash
    python3 tg_email.py
    ```
2.  A message will appear in the console with a URL. Copy this URL and paste it into your web browser.
3.  Choose the Google Account you want the bot to access.
4.  You will see a "Google hasn't verified this app" warning. This is expected. Click **Advanced**, then **Go to [Your App Name] (unsafe)**.
5.  Grant the requested permissions for Gmail.
6.  After you approve, the authentication flow will complete. A new file named `token.json` will be created in your project directory. This file stores your authorization token so you won't have to log in every time.

