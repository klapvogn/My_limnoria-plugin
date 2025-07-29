from supybot import utils, plugins, ircutils, callbacks
from supybot.commands import *
from supybot.i18n import PluginInternationalization
import re
from openai import OpenAI
import subprocess
import json

_ = PluginInternationalization("ChatGPT")

class ChatGPT(callbacks.Plugin):
    """Use the OpenAI ChatGPT API with system command support."""

    threaded = True

    def __init__(self, irc):
        self.__parent = super(ChatGPT, self)
        self.__parent.__init__(irc)
        self.history = {}
        self.client = OpenAI(api_key=self.registryValue("api_key"))

    def bash(self, command: str) -> str:
        """Executes a predefined bash command (with fixed args)."""
        try:
            if command == "df":
                # Run df -h to get human-readable sizes
                result = subprocess.run(["df", "-h", "/"],  # Focus on root filesystem
                                    capture_output=True, text=True)
                if result.returncode != 0:
                    return f"Error: {result.stderr}"
                
                lines = result.stdout.splitlines()
                if len(lines) < 2:
                    return "No disk usage information available."
                
                # Parse the root filesystem line
                parts = lines[1].split()
                if len(parts) >= 6:
                    usage_percent = parts[4].replace('%', '')
                    used = parts[2]
                    available = parts[3]
                    return f"Disk usage is at {usage_percent}% with {used} used and {available} available."
                
                return "Could not parse disk usage information"

            if command == "lsb_release":
                result = subprocess.run(["lsb_release", "-d"], capture_output=True, text=True)
                if result.returncode == 0:
                    # Extract just the description and format consistently
                    description = result.stdout.split("Description:\t")[1].strip()
                    return f"My OS is {description}."
                return "Could not determine OS information"
            # Other commands remain unchanged
            elif command == "lscpu":
                result = subprocess.run(["lscpu"], capture_output=True, text=True)
            elif command == "free":
                result = subprocess.run(["free", "-m"], capture_output=True, text=True)
            elif command == "uname":
                result = subprocess.run(["uname", "-a"], capture_output=True, text=True)                
            elif command == "lshw":
                result = subprocess.run(["lshw", "-short"], capture_output=True, text=True)
          #  elif command == "lsb_release":
          #      result = subprocess.run(["lsb_release", "-a"], capture_output=True, text=True)
            elif command == "uptime":
                result = subprocess.run(["uptime"], capture_output=True, text=True)
            #elif command == "mpstat":
            #    result = subprocess.run(["mpstat"], capture_output=True, text=True)
            else:
                return f"Error: Unsupported command '{command}'."

            return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
        except Exception as e:
            return f"Error executing command: {str(e)}"

    def chat(self, irc, msg, args, text):
        """Manual Call to the ChatGPT API with function calling support."""
        channel = msg.channel
        if not irc.isChannel(channel):
            channel = msg.nick

        if self.registryValue("nick_include", msg.channel):
            text = "%s: %s" % (msg.nick, text)

        self.history.setdefault(channel, [])
        max_history = self.registryValue("max_history", msg.channel)
        prompt = self.registryValue("prompt", msg.channel).replace("$botnick", irc.nick)

        # Initial API call
        response = self.client.chat.completions.create(
            model=self.registryValue("model", msg.channel),
            messages=self.history[channel][-max_history:]
            + [
                {"role": "system", "content": prompt},
                {"role": "user", "content": text},
            ],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "bash",
                        "description": "Run system commands to fetch hardware/OS info.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "command": {
                                    "type": "string",
                                    "enum": [
                                        "lscpu", "free", "lshw",
                                        "uname", "uptime",
                                        "df"
                                    ],
                                }
                            },
                            "required": ["command"],
                        },
                    }
                }
            ],
            tool_choice="auto",
            temperature=self.registryValue("temperature", msg.channel),
            max_tokens=self.registryValue("max_tokens", msg.channel),
        )

        response_message = response.choices[0].message
        content = response_message.content if hasattr(response_message, 'content') else None

        # Handle tool calls
        if response_message.tool_calls:
            for tool_call in response_message.tool_calls:
                if tool_call.function.name == "bash":
                    function_args = json.loads(tool_call.function.arguments)
                    command = function_args["command"]
                    command_output = self.bash(command)
                    
                    # For df command, return raw output without GPT processing
                    if command in ["df", "lsb_release"]:
                        irc.reply(command_output, prefixNick=False)
                        return
                    
                    # For other commands, process normally
                    second_response = self.client.chat.completions.create(
                        model=self.registryValue("model", msg.channel),
                        messages=[
                            {"role": "system", "content": prompt},
                            {"role": "user", "content": text},
                            response_message,
                            {
                                "role": "tool",
                                "name": "bash",
                                "content": command_output,
                                "tool_call_id": tool_call.id
                            },
                        ],
                        temperature=self.registryValue("temperature", msg.channel),
                        max_tokens=self.registryValue("max_tokens", msg.channel),
                    )
                    content = second_response.choices[0].message.content

        # Only process content if it exists
        if content:
            if self.registryValue("nick_strip", msg.channel):
                content = re.sub(r"^%s: " % (irc.nick), "", content)

            prefix = self.registryValue("nick_prefix", msg.channel)
            if self.registryValue("reply_intact", msg.channel):
                for line in content.splitlines():
                    if line:
                        irc.reply(line, prefixNick=prefix)
            else:
                response = " ".join(content.splitlines())
                irc.reply(response, prefixNick=prefix)

            # Update chat history
            self.history[channel].append({"role": "user", "content": text})
            self.history[channel].append({"role": "assistant", "content": content})

    chat = wrap(chat, ["text"])

Class = ChatGPT
