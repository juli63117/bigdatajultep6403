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
package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
	// Определение тэга OutputTag для несопоставленных поездок и тарифов
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {
		// Чтение параметров из командной строки
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData); // Путь к файлу с данными о поездках
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData); // Путь к файлу с данными о тарифах

		final int maxEventDelay = 60;           // События могут быть сдвинуты по времени максимум на 60 секунд
		final int servingSpeedFactor = 600;     // Каждая секунда представляет 10 минут событий

		// Установка окружения для потоковой обработки
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Обработка с учетом времени событий
		env.setParallelism(ExerciseBase.parallelism); // Установка уровня параллелизма

		// Поток данных о поездках
		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor))) // Источник данных о поездках
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0))) // Фильтр поездок
				.keyBy(ride -> ride.rideId); // Ключ по rideId

		// Поток данных о тарифах
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor))) // Источник данных о тарифах
				.keyBy(fare -> fare.rideId); // Ключ по rideId

		// Обогащение данных с помощью функции соединения потоков
		SingleOutputStreamOperator processed = rides
				.connect(fares) // Соединение двух потоков
				.process(new EnrichmentFunction()); // Процессор для обогащения

		// Печать неподтвержденных тарифов
		printOrTest(processed.getSideOutput(unmatchedFares));

		// Запуск обработки
		env.execute("ExpiringStateExercise (java)");
	}

	/**
	 * Функция обогащения данных, которая соединяет поездки и тарифы
	 */
	public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// Состояния для хранения информации о поездке и тарифе
		private ValueState<TaxiRide> taxiRideState;
		private ValueState<TaxiFare> taxiFareState;

		// Инициализация состояний при запуске функции
		@Override
		public void open(Configuration config) throws Exception {
			taxiRideState = getRuntimeContext().getState(new ValueStateDescriptor<>(
					"Taxi Ride", TaxiRide.class // Дескриптор состояния для поездки
			));
			taxiFareState = getRuntimeContext().getState(new ValueStateDescriptor<>(
					"Taxi Fare", TaxiFare.class // Дескриптор состояния для тарифа
			));
		}

		// Обработчик таймеров, который вызывается, когда время события истекает
		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Проверка на наличие и добавка неподтвержденных объектов
			// (поездки или тарифы) в выходные теги
			checkForUnmatched(ctx, out);
		}

		// Вспомогательная функция для проверки и добавления неподтвержденных объектов
		private void checkForUnmatched(OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			if (taxiFareState.value() != null) {
				ctx.output(unmatchedFares, taxiFareState.value());
				taxiFareState.clear(); // Очищение состояния тарифа
			}
			if (taxiRideState.value() != null) {
				ctx.output(unmatchedRides, taxiRideState.value());
				taxiRideState.clear(); // Очищение состояния поездки
			}
		}

		// Обработка элемента из потока поездок
		@Override
		public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = taxiFareState.value(); // Получение тарифа из состояния
			// Поездка соединяется с тарифом, если тариф существует
			if (fare != null) {
				out.collect(new Tuple2<>(ride, fare));
				clearStateAndTimers(fare.getEventTime(), context); // Очищение состояния и удаление таймера
			} else {
				taxiRideState.update(ride); // Сохранение поездки в состояние
				context.timerService().registerEventTimeTimer(ride.getEventTime()); // Таймер
			}
		}

		// Обработка элемента из потока тарифов
		@Override
		public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = taxiRideState.value();
			// Поездка соединяется с тарифом, если поездка существует
			if (ride != null) {
				out.collect(new Tuple2<>(ride, fare));
				clearStateAndTimers(ride.getEventTime(), context);
			} else {
				taxiFareState.update(fare);
				context.timerService().registerEventTimeTimer(fare.getEventTime());
			}
		}

		// Вспомогательная функция для очистки состояний и таймеров
		private void clearStateAndTimers(long eventTime, Context context) throws Exception {
			context.timerService().deleteEventTimeTimer(eventTime);
			taxiRideState.clear();
			taxiFareState.clear();
		}
	}

}