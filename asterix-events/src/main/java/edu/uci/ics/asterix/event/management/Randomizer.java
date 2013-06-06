/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.event.management;

import java.util.Random;
import java.util.logging.Logger;

public class Randomizer {

	private static final Logger LOGGER = Logger.getLogger(Randomizer.class
			.getName());
	private static Randomizer INSTANCE;
	private final Random random;
	private final int seed;

	public static Randomizer getInstance(int seed) {
		if (INSTANCE == null) {
			INSTANCE = new Randomizer(seed);
		}
		return INSTANCE;
	}

	public static Randomizer getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Randomizer();
		}
		return INSTANCE;
	}

	private Randomizer() {
		Random rm = new Random();
		seed = rm.nextInt(10000);
		random = new Random(seed);
		LOGGER.info("SEED:" + seed);
	}

	private Randomizer(int seed) {
		this.seed = seed;
		random = new Random(seed);
		LOGGER.info("SEED:" + seed);
	}

	public Random getRandom() {
		return random;
	}

	public int getSeed() {
		return seed;
	}

	public int getRandomInt(int min, int max) {
		return min + random.nextInt(max - min + 1);
	}
}
