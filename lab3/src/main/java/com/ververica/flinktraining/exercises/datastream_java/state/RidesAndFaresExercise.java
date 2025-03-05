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
package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData); // Путь к файлу с данными о поездках
		final String faresFile = params.get("fares", pathToFareData); // Путь к файлу с данными о тарифах

		// Настройка параметров задержки и скорости обслуживания
		final int delay = 60;                    // Максимальная задержка 60 секунд
		final int servingSpeedFactor = 1800;     // 30 минут данных обрабатываются за каждую секунду

		// Настройка окружения для выполнения стриминга
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// Чтение и фильтрация потока данных о поездках
		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy("rideId");

		// Чтение потока данных о тарифах
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
				.keyBy("rideId");

		// Объединение потоков поездок и тарифов
		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		// Вывод обогащенных данных
		printOrTest(enrichedRides);

		// Запуск выполнения пайплайна
		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	// Функция обогащения данных о поездках с тарифами
	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// Состояния для хранения поездки и тарифа
		private ValueState<TaxiRide> taxiRideState;
		private ValueState<TaxiFare> taxiFareState;

		// Инициализация состояний
		@Override
		public void open(Configuration config) throws Exception {
			taxiRideState = getRuntimeContext().getState(new ValueStateDescriptor<>(
					"Taxi Ride", TaxiRide.class // Создание дескриптора для состояния поездки
			));
			taxiFareState = getRuntimeContext().getState(new ValueStateDescriptor<>(
					"Taxi Fare", TaxiFare.class // Создание дескриптора для состояния тарифа
			));
		}

		// Обработка данных о поездке
		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Получение текущего тарифа из состояния
			TaxiFare taxiFare = taxiFareState.value();
			if (taxiFare != null) {
				// Если найден тариф, состояние очищается и отправляются соединенные данные
				taxiFareState.clear();
				out.collect(new Tuple2<>(ride, taxiFare));
			} else {
				// Если тарифа нет, поездка сохраняется в состояние
				taxiRideState.update(ride);
			}
		}

		// Обработка данных о тарифе
		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Получаение текущей поездка из состояния
			TaxiRide taxiRide = taxiRideState.value();
			if (taxiRide != null) {
				taxiRideState.clear();
				out.collect(new Tuple2<>(taxiRide, fare));
			} else {
				taxiFareState.update(fare);
			}
		}
	}
}
