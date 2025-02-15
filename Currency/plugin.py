import requests
import re
from supybot.commands import wrap
import supybot.callbacks as callbacks

class Currency(callbacks.Plugin):
    def __init__(self, *args, **kwargs):
        super(Currency, self).__init__(*args, **kwargs)

    def fetch_conversion_rate(self, from_currency, to_currency):
        api_key = self.registryValue("apiKey")
        url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{from_currency}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'conversion_rates' in data and to_currency in data['conversion_rates']:
                    return data['conversion_rates'][to_currency]
            return None
        except requests.exceptions.RequestException:
            return None

    def convert_currency(self, irc, msg, args, amount, from_currency, to_currency):
        api_key = self.registryValue("apiKey")
        if not api_key:
            irc.reply("Error: You need to set an API key to use this plugin.")
            return        
        rate = self.fetch_conversion_rate(from_currency, to_currency)
        if rate:
            converted_amount = amount * rate
            irc.reply(f"{amount} {from_currency} is approximately {converted_amount:.2f} {to_currency}.")
        else:
            irc.reply(f"Could not retrieve conversion rate for {from_currency} to {to_currency}. Please try again later.")

    def currency(self, irc, msg, args, input_string):
        """
        Handles the +currency command.
        Usage: +currency <amount> <from_currency> to <to_currency>
        Example: +currency 20 eur to usd
        """
        match = re.match(r"(\d+(?:\.\d+)?)\s+([a-zA-Z]{3})\s+to\s+([a-zA-Z]{3})", input_string)
        if not match:
            irc.reply("Invalid format. Use: +currency <amount> <from_currency> to <to_currency>")
            return
        
        amount, from_currency, to_currency = match.groups()

        try:
            amount = float(amount)
        except ValueError:
            irc.reply("Invalid amount. Please provide a valid numeric amount.")
            return

        self.convert_currency(irc, msg, args, amount, from_currency.upper(), to_currency.upper())

    currency = wrap(currency, ['text'])

Class = Currency