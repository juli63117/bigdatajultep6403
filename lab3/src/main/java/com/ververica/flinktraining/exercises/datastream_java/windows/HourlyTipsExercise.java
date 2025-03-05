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
package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // Максимальная задержка событий — 60 секунд (события могут быть в порядке отставания)
		final int servingSpeedFactor = 600; // Каждые 10 минут данных обрабатываются за 1 секунду

		// Настройка окружения для потоковой обработки
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// Запуск генератора данных (из источника с тарифами)
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// Группировка по идентификатору водителя, вычисление суммы чаевых за каждый час
		DataStream<Tuple3<Long, Long, Float>> hourlyMax =
				fares.keyBy(fare -> fare.driverId) // Группировка по driverId
						.timeWindow(Time.hours(1))   // Окно длительностью 1 час
						.process(new AddHourlyTips()) // Обработка с добавлением суммы чаевых
						.timeWindowAll(Time.hours(1)) // Все окна за 1 час
						.maxBy(2); // Максимальная сумма чаевых

		// Вывод результата: водитель с самой высокой суммой чаевых для каждого часа
		printOrTest(hourlyMax);

		// Запуск выполнения пайплайна преобразований
		env.execute("Hourly Tips (java)");

	}

	// Класс для обработки данных внутри окна
	public static class AddHourlyTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			float sum = 0.0f;
			// Вычисление суммы чаевых за все поездки в пределах окна
			for (TaxiFare fare : fares) {
				sum += fare.tip;
			}
			// Отправка результата: конец окна, ID водителя и сумма чаевых
			out.collect(new Tuple3<>(context.window().getEnd(), key, sum));
		}
	}
}
