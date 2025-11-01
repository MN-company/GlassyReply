![GLASSYREPLY.png](https://github.com/MN-company/GlassyReply/blob/f7143f3a0553dae4b8ea6affcb8d963fd10204b5/GLASSYREPLY.png)

This is an email handling bot through Telegram inspired by hominom (official bot)[https://t.me/GmailBot]. The special feature compared to the official bot is the full and automatic integration of Gemini for response generation through artificial intelligence. The project is entirely replicable for free 

## Features.
**Classic gmail features** 
  - Star / unstar email
  - Enter the ai's response in draft
  - Send the ai's response
  - Trash the email
  - Add a tag to the email
  - Forward the email
**Extra functions**
  - Generating responses automatically with the ai (Gemini API)
  - Inserting a Pixel Tracker with proprietary server (with anti Mail Proxy Server filtering).
**Filters and configurations**
  - Anti generation filter based on email address (e.g., noreply)
  - Tag-based anti generation filter ("update")

---

## Prerequisites

- Python 3.8+
- A Google Account (You have access to Gemini API for free)
- A Telegram Account
- [A Cloudfare account (free tier)](https://dash.cloudflare.com/login)
- [Node.js (npm)](https://github.com/npm/cli)

---

## Setup Instructions


### 1. Clone the Project

```bash
git clone https://github.com/MN-company/GlassyReply.git && cd GlassyReply
```
### 2. Install Dependencies
```bash
pip install -r requirements.txt
```
### 3. Configure Google Cloud & Gmail API

1.  **Go to the Google Cloud Console**: [console.cloud.google.com](https://console.cloud.google.com/)
2.  **Create a new project**: Create a new project
3.  **Enable the Gmail API**:
    - In the search bar, type "Gmail API" and select it.
    - Click the **Enable** button.
4.  **Create Credentials**:
    - Go to the **Credentials** page from the left-hand menu.
    - Click **+ CREATE CREDENTIALS** and select **OAuth client ID**.
    - If prompted, configure the **OAuth consent screen**.
        - Select **External** and click **Create**.
        - App name: `GlassyReply` (or anything you like).
        - User support email: Your email address.
        - Developer contact information: Your email address.
        - Click **Save and Continue** through the Scopes and Test Users sections. You do not need to add test users for this to work.
    - Now, back on the Credentials page, create the OAuth client ID:
        - Application type: **Desktop app**.
        - Name: `GlassyReply Credentials` (or anything you like).
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
    - Search for the bot `@RawDataBot` on Telegram and start a chat.
    - It will immediately send you a message containing your user information, including your **ID**. Copy it.
  
### 6. Cloudfare worker activation.
1. Open [A Cloudfare account (free tier)](https://dash.cloudflare.com/login)
2. In the terminal, type.

``bash
npx wrangler init pixel-worker
``
3. In the ‘src’ folder, enter
  - index.ts
  - types.ts
  - utils.ts
4. Replace the original wrangler.toml with the one downloaded
5. Deploy the worker and a URL will be resituated
``bash
npx wrangler deploy
``
6. The URL will then be placed in the .env file

Translated with DeepL.com (free version)


### 7. Create the Environment File

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

