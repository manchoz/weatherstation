/*
 * Copyright 2016 The Android Open Source Project and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *      Benjamin Cabé <benjamin@eclipse.org> - Adapt PubSubPublisher for MQTT
 *
 */

package com.example.androidthings.weatherstation;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Base64;
import android.util.Log;

import com.google.api.client.http.HttpTransport;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;

import org.eclipse.paho.android.service.MqttAndroidClient;
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

class MqttPublisher {
    private static final String TAG = MqttPublisher.class.getSimpleName();

    private final Context mContext;
    private final String mAppname;
    private final String mTopic;

    private Handler mHandler;
    private HandlerThread mHandlerThread;

    private float mLastTemperature = Float.NaN;
    private float mLastPressure = Float.NaN;

    private static final long PUBLISH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

    private MqttAndroidClient mMqttAndroidClient;
    private static final String MQTT_SERVER_URI = "tcp://iot.eclipse.org:1883";

    MqttPublisher(Context context, String appname, String topic) throws IOException {
        mContext = context;
        mAppname = appname;
        mTopic = topic;

        mHandlerThread = new HandlerThread("mqttPublisherThread");
        mHandlerThread.start();
        mHandler = new Handler(mHandlerThread.getLooper());

        mMqttAndroidClient = new MqttAndroidClient(mContext, MQTT_SERVER_URI, MqttClient.generateClientId());
        mMqttAndroidClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                Log.d(TAG, "MQTT connection complete");
            }

            @Override
            public void connectionLost(Throwable cause) {
                Log.d(TAG, "MQTT connection lost");
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                Log.d(TAG, "MQTT message arrived");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                Log.d(TAG, "MQTT delivery complete");
            }
        });

        final MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setAutomaticReconnect(true);
        mqttConnectOptions.setCleanSession(false);


        mHandler.post(new Runnable() {
            @Override
            public void run() {
            // Connect to the broker
                try {
                    mMqttAndroidClient.connect(mqttConnectOptions, null, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            DisconnectedBufferOptions disconnectedBufferOptions = new DisconnectedBufferOptions();
                            disconnectedBufferOptions.setBufferEnabled(true);
                            disconnectedBufferOptions.setBufferSize(100);
                            disconnectedBufferOptions.setPersistBuffer(false);
                            disconnectedBufferOptions.setDeleteOldestMessages(false);
                            mMqttAndroidClient.setBufferOpts(disconnectedBufferOptions);
                        }

                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                            Log.d(TAG, "MQTT connection failure", exception);
                        }
                    });
                } catch (MqttException e) {
                    Log.d(TAG, "MQTT connection failure", e);
                }

            }
        });
    }

    public void start() {
        mHandler.post(mPublishRunnable);
    }

    public void stop() {
        mHandler.removeCallbacks(mPublishRunnable);
    }

    public void close() {
        mHandler.removeCallbacks(mPublishRunnable);
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    mMqttAndroidClient.disconnect();
                } catch (MqttException e) {
                    Log.d(TAG, "error disconnecting MQTT client");
                } finally {
                    mMqttAndroidClient = null;
                }
            }
        });
        mHandlerThread.quitSafely();
    }

    public SensorEventListener getTemperatureListener() {
        return mTemperatureListener;
    }

    public SensorEventListener getPressureListener() {
        return mPressureListener;
    }

    private Runnable mPublishRunnable = new Runnable() {
        @Override
        public void run() {
            ConnectivityManager connectivityManager =
                    (ConnectivityManager) mContext.getSystemService(Context.CONNECTIVITY_SERVICE);
            NetworkInfo activeNetwork = connectivityManager.getActiveNetworkInfo();
            if (activeNetwork == null || !activeNetwork.isConnectedOrConnecting()) {
                Log.e(TAG, "no active network");
                return;
            }

            try {
                JSONObject messagePayload = createMessagePayload(mLastTemperature, mLastPressure);
                if (!messagePayload.has("data")) {
                    Log.d(TAG, "no sensor measurement to publish");
                    return;
                }
                Log.d(TAG, "publishing message: " + messagePayload);
                MqttMessage m = new MqttMessage();
                m.setPayload(messagePayload.toString().getBytes());
                m.setQos(1);
                mMqttAndroidClient.publish(mTopic, m);
            } catch (JSONException | MqttException e) {
                Log.e(TAG, "Error publishing message", e);
            } finally {
                mHandler.postDelayed(mPublishRunnable, PUBLISH_INTERVAL_MS);
            }
        }

        private JSONObject createMessagePayload(float temperature, float pressure)
                throws JSONException {
            JSONObject sensorData = new JSONObject();
            if (!Float.isNaN(temperature)) {
                sensorData.put("temperature", String.valueOf(temperature));
            }
            if (!Float.isNaN(pressure)) {
                sensorData.put("pressure", String.valueOf(pressure));
            }
            JSONObject messagePayload = new JSONObject();
            messagePayload.put("deviceId", Build.DEVICE);
            messagePayload.put("channel", "pubsub");
            messagePayload.put("timestamp", System.currentTimeMillis());
            if (sensorData.has("temperature") || sensorData.has("pressure")) {
                messagePayload.put("data", sensorData);
            }
            return messagePayload;
        }
    };

    private SensorEventListener mTemperatureListener = new SensorEventListener() {
        @Override
        public void onSensorChanged(SensorEvent event) {
            mLastTemperature = event.values[0];
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {}
    };

    private SensorEventListener mPressureListener = new SensorEventListener() {
        @Override
        public void onSensorChanged(SensorEvent event) {
            mLastPressure = event.values[0];
        }

        @Override
        public void onAccuracyChanged(Sensor sensor, int accuracy) {}
    };
}