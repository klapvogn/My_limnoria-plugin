## Spotify

I made this plugin as a fun thing. So when you listen to Spotify you can announce it to a channel with `+playing`


## What you need

client_id and client_secret from : https://developer.spotify.com/

1: Login and navigate to : https://developer.spotify.com/dashboard

2: If you don't have any apps, then make one


I used this curl command to get my tokens

obtaining the `authorization_code`

Step 1:

`curl -X POST "https://accounts.spotify.com/api/token" -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=client_credentials&client_id=<FILL_IT_OUT>&client_secret=<FILL_IT_OUT>"`

Step 2:

`curl -X POST -d "grant_type=authorization_code&code=<FILL_IT_OUT>&redirect_uri=http://localhost:8080&client_id=<FILL_IT_OUT>&client_secret=<FILL_IT_OUT>" https://accounts.spotify.com/api/token`

Then you need to fill out your `spotify_credentials.json` with the content from above `client_id` `client_secret` and `refresh_token` 

`refresh_token` is what you get from step 2

Limnoria have this fancy capabilities I would recommend adding : `+defaultcapability add -Spotify` (+) depending on your config.
As this plugin only works for you, and when other tries to do `+np` they will get : `Error: You don't have the spotify capability. If you think that you should have this capability, be sure that you are identified before trying again. The 'whoami' command can tell you if you're identified.`

when you have done all the above, you can reload the bot. But I would recommend you to restart your bot.

Now you should be able to do: `+np` (+) depending on your config
