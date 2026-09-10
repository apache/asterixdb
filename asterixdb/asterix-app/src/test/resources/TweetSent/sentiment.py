# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import numpy as np

# A toy, deterministic lexicon classifier: no trained model to go stale, no compiled
# dependencies (scikit-learn/scipy) whose ABI can drift out from under a pickled model.
_POSITIVE = np.array(["good", "great", "love", "happy", "awesome", "best", "nice", "thanks", "lol", "haha"])
_NEGATIVE = np.array(["bad", "hate", "sad", "worst", "sucks", "angry", "terrible", "stupid", "ugh", "kill"])


class TweetSent(object):

    def _score(self, text):
        words = np.array(str(text).lower().split())
        if words.size == 0:
            return 0
        pos = np.isin(words, _POSITIVE).sum()
        neg = np.isin(words, _NEGATIVE).sum()
        return int(pos > neg)

    def sentiment(self, args):
        if args is None:
            return 2
        return self._score(args)

    def sentiment_batch(self, args):
        if args is None:
            return 2
        return [self._score(a) for a in args]
