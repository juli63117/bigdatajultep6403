/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */
public class RideCleansingExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// Получение параметров из аргументов командной строки
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // события могут быть не по порядку максимум на 60 секунд
		final int servingSpeedFactor = 600; // события 10 минут обрабатываются за 1 секунду

		// Настраивание окружения для потоковой обработки
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// Создание источника данных из файла с данными о поездках
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// Фильтр потока данных (остаются только поездки, начинающиеся и заканчивающиеся в NYC)
		DataStream<TaxiRide> filteredRides = rides
				.filter(new NYCFilter());
		printOrTest(filteredRides);

		// Конвейер очистки данных
		env.execute("Taxi Ride Cleansing");
	}

	/**
	 *  Внутренний класс, реализующий функцию фильтрации поездок, которые не начинаются и не заканчиваются в NYC.
	 */
	private static class NYCFilter implements FilterFunction<TaxiRide> {
		/**
		 *  Функция фильтрации, которая определяет, находится ли поездка в пределах NYC.
		 *  @param taxiRide Поездка в такси для проверки.
		 *  @return true, если поездка начинается и заканчивается в NYC, иначе false.
		 */
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}