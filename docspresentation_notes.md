

\## Dashboard Analytics



The dashboard has two levels.



The live map shows individual taxi movement.



The analytics page shows fleet-level behavior.



Fleet behavior means the overall pattern of all taxis together, such as active taxis, total distance, top speed taxis, top distance taxis, and incident types.



In the analytics view, the fleet summary shows the total active taxis, total distance, total incidents, and overspeed alerts.



The top average speed chart helps us identify taxis with unusual driving behavior.



The top distance chart shows which taxis contributed most to fleet movement.



The incident type chart shows which type of alert happened most often, such as overspeed, 10 km warning, or 15 km discard.



The numbers grow quickly because the producer replays historical data faster than real time.



\## Clicking an Individual Taxi



When we click one taxi, the dashboard moves from fleet-level view to taxi-level view.



It shows taxi id, marker color, current status, zone, distance from center, average speed, total distance, latitude, longitude, and last timestamp.



The GPS position comes from the processed stream.



Speed, distance, zone, and alerts are calculated in Flink.



The latest taxi state is served to the dashboard through the API and Redis.



This helps inspect one taxi after seeing a marker or incident.



\## Dataset



The dataset is the T-Drive taxi trajectory dataset from Beijing.



It is not an edge list or road graph.



It contains raw GPS trajectory data.



Each row is one GPS ping.



Each row has taxi id, timestamp, longitude, and latitude.



The dataset does not directly contain speed, distance, alerts, or average speed.



We calculate those values in Flink by comparing consecutive GPS points for the same taxi.



\## 5-Second Propagation



Flink still processes every valid GPS point.



But the dashboard receives one location update per taxi every 5 seconds.



This avoids sending every GPS point to the browser.



It keeps the map smooth and avoids too many WebSocket messages.



This does not affect speed, distance, or alert calculation.



Alerts are still calculated from every valid GPS point.



\## Benchmark Script



The benchmark script is a Kafka consumer.



It listens to output topics such as taxi-locations, violation-events, and fleet-metrics.



It counts how many messages arrive during a fixed time window.



From this, it reports total messages, average throughput, peak throughput, and per-topic totals.



It does not change the pipeline.



It only observes the processed output.



\## Latency



The 150 ms value is Flink processing latency.



It means one GPS event takes around 150 ms to pass through the Flink processing pipeline after optimization.



It is not full dashboard latency.



Full dashboard latency is higher because it includes producer, Kafka, Flink, Redis, API, WebSocket, and the 5-second dashboard propagation interval.



\## Operator Chaining



Flink already provides operator chaining.



We did not create a new chaining feature.



Our contribution was making the pipeline chaining-friendly.



We kept related processing steps in one continuous Flink job and reduced unnecessary intermediate boundaries.



This means the same GPS event moves more directly through cleaning, speed calculation, distance calculation, alerts, fleet statistics, and storage.



The logic stays the same, but overhead is lower.



Simple phrase: same logic, fewer boundaries.



\## Final Takeaway



The project is an end-to-end real-time traffic monitoring system.



Kafka ingests taxi GPS data.



Flink calculates speed, distance, average speed, geofence alerts, and overspeed alerts.



Redis stores live state.



FastAPI serves the data.



The dashboard shows live movement, incidents, analytics, and taxi-level details.



One command starts the full stack.

