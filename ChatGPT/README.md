Use the OpenAI ChatGPT API

This plugin is under development and probably shouldn't be used by anyone...

Get an API key from https://platform.openai.com/account/api-keys
```
@config plugins.chatgpt.api_key YOUR_KEY_HERE
```

History:
```
@config plugins.chatgpt.max_history 10
```
^^ Use this config to set maximum number of messages to keep in your history. 0 to disable.

System Prompt:
```
@config plugins.chatgpt.prompt "$botnick You're a knowledgeable assistant. Include relevant URLs as plain text, no Markdown. **Enclose Linux commands and file paths in backticks** for clarity. Be brief, and helpful; not solicitous. Keep lines under 400 characters. No prefix."
```
^^ Configurable per channel, etc. get creative

```
@config list plugins.chatgpt
```
^^ Please take a look at the various options and configure stuff before you do anything.

```
@chat <text>
```
^^ Command to send text to the chatgpt API

```
@messageparser add "(?i)(.*BOT_NICK_HERE.*)" "chat $1"
```
^^ replace BOT_NICK_HERE with your bot nick and add automatic replies to nick mentions
