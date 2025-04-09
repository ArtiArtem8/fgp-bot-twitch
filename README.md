# fgp-bot-twitch

# 🎮 Twitch Bot for yabloko18 Channel

A feature-rich Twitch bot designed specifically for [twitch.tv/yabloko18](https://www.twitch.tv/yabloko18), built with modularity and extensibility in mind using TwitchIO.


## Features

- **Follow Age Command**  
  Allows users to check how long they or another user have been following the channel, with output formatted in Russian.

- **Message Logging**  
  Logs chat messages, including details like badges, subscription status, follower status, and message metadata into an SQLite database for later analysis.

- **Social Commands**  
  Provides commands to share social media links (e.g., Discord and Telegram) and a command to ban users from the chat.


## Requirements

- **Python Dependencies:**
  - `asqlite` @ git+https://github.com/Rapptz/asqlite.git@8d929c7628e53d01b6905866f8b6d350e76a8d40
  - `twitchio==3.0.0b4`
  - `python-dotenv==1.1.0`

- **Environment Variables:**  
  The project uses environment variables to store sensitive data. Ensure you have a `.env` file with the following variables configured:
  - `TWITCH_BOT_APP_CLIENT_SECRET`
  - `TWITCH_BOT_ID`
  - `TWITCH_OWNER_ID`
  - `TWITCH_BOT_APP_CLIENT_ID`

## Overview

**fgp-bot-twitch** leverages the [twitchio](https://github.com/TwitchIO/TwitchIO) library to interface with Twitch's API, while using [asqlite](https://github.com/Rapptz/asqlite) for asynchronous SQLite database operations. The bot is structured around modular components, making it easy to add or modify functionalities in the future.

---

## Contributing

Contributions are welcome! Whether you're adding new features, fixing bugs, or improving documentation, feel free to fork the repository and submit a pull request.

---

## License

This project is open-source and available under the [MIT License](LICENSE).