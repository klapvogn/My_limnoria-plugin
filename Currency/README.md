## Currency

What is Currency?

I made a Currency plugin that can convert any choosen currency.

What you need for this plugin to work:

1: An API from https://www.exchangerate-api.com

What now?

You now have to add the plugin into your plugin directory

Then you need to load the plugin:

`+load currency`

Then:

`+supybot.plugins.Currency.apiKey <your_api_key_here>`

And lastly:

`+reload currency`

And you should be good to go!

Output

`+currency 20 eur to usd`

`20.0 EUR is approximately 20.98 USD.`
