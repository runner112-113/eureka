/*
 * Copyright 2014 Netflix, Inc.
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
 */

package com.netflix.discovery.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate limiter implementation is based on token bucket algorithm. There are two parameters:
 * <ul>
 * <li>
 *     burst size - maximum number of requests allowed into the system as a burst
 *     允许以突发方式进入系统的最大请求数
 * </li>
 * <li>
 *     average rate - expected number of requests per second (RateLimiters using MINUTES is also supported)
 *     设置的时间窗口内允许进入的请求数
 * </li>
 * </ul>
 *
 * @author Tomasz Bak
 */
public class RateLimiter {

    private final long rateToMsConversion;

    private final AtomicInteger consumedTokens = new AtomicInteger();
    private final AtomicLong lastRefillTime = new AtomicLong(0);

    @Deprecated
    public RateLimiter() {
        this(TimeUnit.SECONDS);
    }

    public RateLimiter(TimeUnit averageRateUnit) {
        switch (averageRateUnit) {
            case SECONDS:
                rateToMsConversion = 1000;
                break;
            case MINUTES:
                rateToMsConversion = 60 * 1000;
                break;
            default:
                throw new IllegalArgumentException("TimeUnit of " + averageRateUnit + " is not supported");
        }
    }

    public boolean acquire(int burstSize, long averageRate) {
        return acquire(burstSize, averageRate, System.currentTimeMillis());
    }

    public boolean acquire(int burstSize, long averageRate, long currentTimeMillis) {
        if (burstSize <= 0 || averageRate <= 0) { // Instead of throwing exception, we just let all the traffic go
            return true;
        }

        refillToken(burstSize, averageRate, currentTimeMillis);
        return consumeToken(burstSize);
    }

    private void refillToken(int burstSize, long averageRate, long currentTimeMillis) {
        // 上一次填充 token 的时间
        long refillTime = lastRefillTime.get();
        // 时间差
        long timeDelta = currentTimeMillis - refillTime;

        // 固定生成令牌的速率，即每分钟4次
        // 例如刚好间隔15秒进来一个请求，就是 15000 * 4 / 60000 = 1，newTokens 代表间隔了多少次，如果等于0，说明间隔不足15秒
        long newTokens = timeDelta * averageRate / rateToMsConversion;
        if (newTokens > 0) {
            long newRefillTime = refillTime == 0
                    ? currentTimeMillis
                    // 注意这里不是直接设置的当前时间戳，而是根据 newTokens 重新计算的，因为有可能同一周期内同时有多个请求进来，这样可以保持一个固定的周期
                    : refillTime + newTokens * rateToMsConversion / averageRate;
            if (lastRefillTime.compareAndSet(refillTime, newRefillTime)) {
                while (true) {
                    // 调整令牌的数量
                    int currentLevel = consumedTokens.get();
                    int adjustedLevel = Math.min(currentLevel, burstSize); // In case burstSize decreased
                    // currentLevel 可能为2，重置为了 0 或 1
                    int newLevel = (int) Math.max(0, adjustedLevel - newTokens);
                    if (consumedTokens.compareAndSet(currentLevel, newLevel)) {
                        return;
                    }
                }
            }
        }
    }

    private boolean consumeToken(int burstSize) {
        while (true) {
            int currentLevel = consumedTokens.get();
            // 突发数量为2，也就是允许15秒内最多有两次请求进来
            if (currentLevel >= burstSize) {
                return false;
            }
            if (consumedTokens.compareAndSet(currentLevel, currentLevel + 1)) {
                return true;
            }
        }
    }

    public void reset() {
        consumedTokens.set(0);
        lastRefillTime.set(0);
    }
}
