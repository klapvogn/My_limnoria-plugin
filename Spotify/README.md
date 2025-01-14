## Spotify

I made this plugin as a fun thing. So when you listen to Spotify you can announce it to a channel with `+playing`


## What you need

client_id and client_secret from : https://developer.spotify.com/

1: Login and navigate to : https://developer.spotify.com/dashboard

2: If you don't have any apps, then make one


I used this curl command to get my tokens

obtaining the `authorization_code`

`curl -X POST "https://accounts.spotify.com/api/token" -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=client_credentials&client_id=<FILL_IT_OUT>&client_secret=<FILL_IT_OUT>"`

`curl -X POST -d "grant_type=authorization_code&code=<FILL_IT_OUT>&redirect_uri=http://localhost:8080&client_id=<FILL_IT_OUT>&client_secret=<FILL_IT_OUT>" https://accounts.spotify.com/api/token`
