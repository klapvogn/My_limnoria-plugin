import re
import requests
from supybot.commands import *
import supybot.plugins as plugins
import supybot.ircutils as ircutils
import supybot.callbacks as callbacks

class TVmaze(callbacks.Plugin):
    """Fetches TV show information from TVmaze API"""
    threaded = True

    def _clean_html(self, text):
        """Remove HTML tags from a string"""
        if not text:
            return "No summary available"
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
    def _smart_truncate(self, text, max_length=400):
        """Truncate text at the nearest sentence end before max_length."""
        if len(text) <= max_length:
            return text
        
        # Find the last sentence end (.!?) before max_length
        truncate_at = max_length
        for match in re.finditer(r'[.!?]', text[:max_length + 1]):
            truncate_at = match.end()
        
        truncated = text[:truncate_at].strip()
        if len(text) > truncate_at:
            truncated += " [...]"  # Indicate continuation
        return truncated    

    def tv(self, irc, msg, args, query):
        """<TV show name>
        
        Fetches information about a TV show from TVmaze.
        Example: +tvmaze The Simpsons
        """
        try:
            # Make the API request
            url = 'http://api.tvmaze.com/singlesearch/shows'
            params = {'q': query, 'embed[]': ['cast', 'seasons']}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Extract and clean information
            # Name
            name = data.get('name', 'Unknown')
            premiered = data.get('premiered', 'Unknown')[:4] if data.get('premiered') else 'Unknown'
            # Status
            status = data.get('status', 'Unknown')
            # Network
            network = 'Unknown'
            if data.get('network'):
                network = data.get('network', {}).get('name', 'Unknown')
            elif data.get('webChannel'):
                network = data.get('webChannel', {}).get('name', 'Unknown (Web)')

            # Rating
            rating = data.get('rating', {}).get('average', 'N/A')

            # Format genres (convert list to comma-separated string)
            genres = data.get('genres', [])
            genres_str = ', '.join(genres) if genres else 'N/A'            
            
            # Get number of seasons (from embedded data)
            seasons = data.get('_embedded', {}).get('seasons', [])
            num_seasons = len(seasons) if seasons else 'N/A'

            # Smart summary truncation
            # Summary
            summary = self._clean_html(data.get('summary', 'No summary available')).strip()            
            clean_summary = self._smart_truncate(summary)

            # Format the response
            response = (
                f"{ircutils.bold(name)} ({premiered}) - {status} on {network} | "
                f"{ircutils.bold('Seasons:')} {num_seasons} | Rating: {rating}/10 | {ircutils.bold('Genres:')} {genres_str} | "
                f"{ircutils.bold('Summary:')} {clean_summary}"
            )       
            
            irc.reply(response)
            
        except requests.exceptions.HTTPError:
            irc.reply(f"Show not found: {query}")
        except requests.exceptions.RequestException as e:
            irc.reply(f"Error connecting to TVmaze: {str(e)}")
        except Exception as e:
            irc.reply(f"Error fetching show information: {str(e)}")

    tv = wrap(tv, ['text'])

Class = TVmaze