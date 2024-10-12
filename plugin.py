###
# Copyright (c) 2010, quantumlemur
# Copyright (c) 2011-2023, Valentin Lorentz
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions, and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions, and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the author of this software nor the name of
#     contributors to this software may be used to endorse or promote products
#     derived from this software without specific prior written consent.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

###

import supybot.utils as utils
from supybot.commands import *
import supybot.callbacks as callbacks

import random
import time
import threading
import nltk
import os

def get_nltk_data_path():
    """Determine the path where NLTK data should be stored."""
    return os.path.join(os.path.dirname(__file__), 'nltk_data')

# Set the environment variable and ensure NLTK knows about this path
nltk_data_path = get_nltk_data_path()
os.environ['NLTK_DATA'] = nltk_data_path
nltk.data.path.append(nltk_data_path)

# Ensure 'words' corpus is available
if not os.path.exists(os.path.join(nltk_data_path, 'corpora', 'words.zip')):
    nltk.download('words', download_dir=nltk_data_path)

class Wordies(callbacks.Plugin):
    """A simple word game plugin to find the longest word from random letters."""
    threaded = True

    def __init__(self, irc):
        super().__init__(irc)
        self.submissions = {}
        self.lock = threading.Lock()
        self.game_active = False
        self.letters = []

    def puzzle(self, irc, msg, args):
        """Usage: puzzle
        Generates a set of random letters and asks users to find the longest word within 30 seconds.
        """
        with self.lock:
            if self.game_active:
                irc.reply("A game is already in progress. Please wait until it\'s finished.")
                return

            self.game_active = True
            self.submissions = {}

        # Generate random letters
        self.letters = random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 9)
        letters_str = ' '.join(self.letters)
        irc.reply(f"⏱ ? > Letters: Make the longest word from these letters in 30 seconds: {letters_str}")
        
        max_duration = 30  # seconds

        # Find the longest possible word
        def get_longest_word(letters):
            letters = [letter.lower() for letter in letters]
            valid_words = [word for word in nltk.corpus.words.words() if len(word) > 1 and all(word.count(c) <= letters.count(c) for c in word)]
            valid_words.sort(key=len, reverse=True)
            return valid_words[0] if valid_words else ''

        longest_word = get_longest_word(self.letters)

        def submissions_handler():
            time.sleep(max_duration)
            with self.lock:
                best_submission = ''
                best_nick = ''
                for nick, word in self.submissions.items():
                    if len(word) > len(best_submission):
                        best_submission = word
                        best_nick = nick

                if best_submission:
                    irc.reply(f"Time\'s up and {best_nick} had the best answer \"{best_submission}\", not to spoild the fun but you could have guessed \"{longest_word}\" to ^_^")
                else:
                    irc.reply(f"Time\'s up! The longest word you could have made was: \"{longest_word}\"")

                self.submissions.clear()
                self.game_active = False

        threading.Thread(target=submissions_handler).start()

    def doPrivmsg(self, irc, msg):
        with self.lock:
            if self.game_active and msg.args[0].startswith('#'):
                word = msg.args[1].strip().lower()
                nick = msg.nick
                letters = [letter.lower() for letter in self.letters]

                if all(word.count(c) <= letters.count(c) for c in word) and word in nltk.corpus.words.words():
                    self.submissions[nick] = word

Class = Wordies