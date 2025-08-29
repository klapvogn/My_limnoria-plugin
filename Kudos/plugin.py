import re
import sqlite3
from supybot import plugins, ircutils, callbacks
from supybot.commands import wrap  # Import wrap for command handling

class Kudos(callbacks.Plugin):
    """This plugin lets users award positive or negative kudos points and stores the data in an SQLite3 database."""

    def __init__(self, irc):
        self.__parent = super(Kudos, self)
        self.__parent.__init__(irc)
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database."""
        self.conn = sqlite3.connect('/home/klapvogn/limnoria/plugins/Kudos/kudos.db')  # Create or connect to a database file
        self.cursor = self.conn.cursor()

        # Create a table if it doesn't exist with fields for different kudos types
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS kudos (
                nick TEXT PRIMARY KEY,
                positive INTEGER DEFAULT 0,
                negative INTEGER DEFAULT 0,
                points INTEGER DEFAULT 0,
                informative_pos INTEGER DEFAULT 0,
                funny_pos INTEGER DEFAULT 0,
                nerd_pos INTEGER DEFAULT 0,
                troll_pos INTEGER DEFAULT 0,
                wrong_neg INTEGER DEFAULT 0,
                troll_neg INTEGER DEFAULT 0
            )
        ''')
        self.conn.commit()
        

    def _normalize_nick(self, nick):
        """Normalize the nickname (e.g., remove special characters like '+')."""
        return re.sub(r'[^a-zA-Z0-9]', '', nick.lower())

    def _get_score(self, nick):
        """\x02Helper function to get the current score for a user from the database.\x02"""
        norm_nick = self._normalize_nick(nick)
        self.cursor.execute('''
            SELECT positive, negative, points, informative_pos, funny_pos, nerd_pos, troll_pos, wrong_neg, troll_neg
            FROM kudos WHERE nick = ?
        ''', (norm_nick,))
        row = self.cursor.fetchone()
        if row:
            return {
                "positive": row[0], "negative": row[1], "points": row[2],
                "informative_pos": row[3], "funny_pos": row[4], "nerd_pos": row[5], "troll_pos": row[6],
                "wrong_neg": row[7], "troll_neg": row[8]
            }
        else:
            return {
                "positive": 0, "negative": 0, "points": 0,
                "informative_pos": 0, "funny_pos": 0, "nerd_pos": 0, "troll_pos": 0,
                "wrong_neg": 0, "troll_neg": 0
            }

    def _update_score(self, nick, pos_change=0, neg_change=0, point_change=0, category=None, is_positive=True):
        """Helper function to update a user's score in the database."""
        norm_nick = self._normalize_nick(nick)
        current_score = self._get_score(norm_nick)

        # Update the user's score in memory
        new_positive = current_score['positive'] + (pos_change if is_positive else 0)
        new_negative = current_score['negative'] + (neg_change if not is_positive else 0)
        new_points = current_score['points'] + point_change

        # Update category-specific fields
        category_updates = {
            'informative_pos': current_score['informative_pos'] + (1 if category == 'i' and is_positive else 0),
            'funny_pos': current_score['funny_pos'] + (1 if category == 'f' and is_positive else 0),
            'nerd_pos': current_score['nerd_pos'] + (1 if category == 'n' and is_positive else 0),
            'troll_pos': current_score['troll_pos'] + (1 if category == 't' and is_positive else 0),
            'wrong_neg': current_score['wrong_neg'] + (1 if category == 'w' and not is_positive else 0),
            'troll_neg': current_score['troll_neg'] + (1 if category == 't' and not is_positive else 0)
        }

        # Insert or update the database record
        self.cursor.execute('''
            INSERT INTO kudos (nick, positive, negative, points, informative_pos, funny_pos, nerd_pos, troll_pos, wrong_neg, troll_neg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nick) DO UPDATE SET
                positive = excluded.positive,
                negative = excluded.negative,
                points = excluded.points,
                informative_pos = excluded.informative_pos,
                funny_pos = excluded.funny_pos,
                nerd_pos = excluded.nerd_pos,
                troll_pos = excluded.troll_pos,
                wrong_neg = excluded.wrong_neg,
                troll_neg = excluded.troll_neg
        ''', (
            norm_nick, new_positive, new_negative, new_points,
            category_updates['informative_pos'], category_updates['funny_pos'], category_updates['nerd_pos'],
            category_updates['troll_pos'], category_updates['wrong_neg'], category_updates['troll_neg']
        ))

        self.conn.commit()

    def _parse_kudos(self, irc, msg, text):
        """Parses a kudos message like 'nick++f'."""
        # Use lookahead instead of word boundary at the end
        pattern = re.compile(r'\b([a-zA-Z0-9_\[\]\\^{}|`-]+)(\+\+|\-\-)([ifntw]*)(?=\s|$|\.|,|;)')
        match = pattern.search(text)

        if match:
            nick = match.group(1)
            kudos_cmd = match.group(2)
            category = match.group(3)

            # Additional safety check: ensure it's not part of common URL patterns
            if re.search(r'https?://|www\.|\.(com|org|net|edu|gov)|/', text):
                # If the text contains URL indicators, be more careful
                context = text[max(0, match.start()-10):min(len(text), match.end()+10)]
                if re.search(r'[:/\.\?=]', context):
                    return  # Probably part of a URL

            user = msg.nick
            if user == nick:
                irc.reply("\x02You can't give kudos to yourself.\x02")
                return

            pos_increment = 0
            neg_increment = 0

            if kudos_cmd == "++":
                pos_increment = 1
                self._update_score(nick, pos_change=1, point_change=1, category=category, is_positive=True)
            elif kudos_cmd == "--":
                neg_increment = 1
                self._update_score(nick, neg_change=1, point_change=-1, category=category, is_positive=False)

            # Get updated scores for response
            updated_score = self._get_score(nick)
            positive = updated_score['positive']
            negative = updated_score['negative']
            net = updated_score['points']

            # Format the response
            category_text = {
                'i': 'Informative',
                'f': 'Funny',
                'n': 'Nerd',
                't': 'Troll',
                'w': 'Wrong'
            }.get(category, 'Kudos')

            irc.reply(f"{nick}: +{positive}, -{negative} = {net}. {category_text} ({pos_increment or neg_increment})")       

    def doPrivmsg(self, irc, msg):
        """Intercept all messages and look for kudos-like patterns."""
        text = msg.args[1]
        self._parse_kudos(irc, msg, text)      

    def score(self, irc, msg, args, nick):
        """\x02[nick] (Displays the current kudos score of nick.)\x02"""
        score = self._get_score(nick)
        irc.reply(f"{nick} has {score['positive']} \x033Positive\x03 and {score['negative']} \x034Negative\x03 kudos. (Net score: {score['points']})")     

    score = wrap(score, ['nick'])

    def scores(self, irc, msg, args):
        """Displays the kudos scores of all users in detailed format."""
        self.cursor.execute('SELECT nick, positive, negative, points, informative_pos, funny_pos, nerd_pos, troll_pos, wrong_neg, troll_neg FROM kudos ORDER BY points DESC')
        rows = self.cursor.fetchall()

        if not rows:
            irc.reply("\x02No kudos have been awarded yet.\x02")
            return

        scores_list = []
        for row in rows:
            nick, positive, negative, points, informative, funny, nerd, troll_pos, wrong, troll_neg = row
            breakdown = (f"(\x033Informative\x03: {informative} - \x033Funny\x03: {funny} - \x033Nerd\x03: {nerd} - \x033Troll\x03: {troll_pos}) / "
                         f"(\x034Wrong\x03: -{wrong} - \x034Troll\x03: -{troll_neg})")
            scores_list.append(f"{nick}: {points} {breakdown} (\x033Positive\x03: {positive}, \x034Negative\x03: {negative})")

        irc.reply(" | ".join(scores_list))

    scores = wrap(scores, [])

    def kudos(self, irc, msg, args):
        """Displays help information about the Kudos plugin."""
        help_message = (
            "\x02Kudos is used to award positive and negative kudo points to users in a channel. "
            "e.g. bob++ will award a positive point to bob, and -- a negative point. "
            "More than one point may be awarded by using, for example, bob+++ or bob+=2, however, "
            "this is limited by your current level of kudos (positive-negative kudos); "
            "1 additional point for every 50 kudo points you have (max 3). You can also rate \x02"
        )
        irc.reply(help_message)

    kudos = wrap(kudos, [])    

    def die(self):
        """Called when the plugin is disabled."""
        self.conn.close()  # Close the database connection

Class = Kudos
