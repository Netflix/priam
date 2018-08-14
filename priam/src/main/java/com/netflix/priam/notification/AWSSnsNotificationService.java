/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.notification;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.IAMCredential;
import com.netflix.priam.merics.IMeasurement;
import com.netflix.priam.merics.NotificationMeasurement;
import com.netflix.priam.utils.BoundedExponentialRetryCallable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
 * A single, persisted, connection to Amazon SNS.
 */
@Singleton
public class AWSSnsNotificationService implements INotificationService {
	private static final Logger logger = LoggerFactory.getLogger(AWSSnsNotificationService.class);

	private IConfiguration configuration;
	private AmazonSNS snsClient;
	private IMeasurement notificationMeasurement;
	
	@Inject
	public AWSSnsNotificationService(IConfiguration config, IAMCredential iamCredential
			, NotificationMeasurement notificationMeasurement) {
		this.configuration = config;
		this.notificationMeasurement = notificationMeasurement;
		String ec2_region = this.configuration.getDC();
		snsClient = AmazonSNSClient.builder()
				.withCredentials(iamCredential.getAwsCredentialProvider())
				.withRegion(ec2_region).build();
	}
	
	@Override
	public void notify(final String msg, final Map<String, MessageAttributeValue> messageAttributes) {
		final String topic_arn = this.configuration.getBackupNotificationTopicArn(); //e.g. arn:aws:sns:eu-west-1:1234:eu-west-1-cass-sample-backup
		if (StringUtils.isEmpty(topic_arn)) {
			return;
		}
		
		PublishResult publishResult = null;
		try {
			publishResult = new BoundedExponentialRetryCallable<PublishResult>() {
				@Override
				public PublishResult retriableCall() throws Exception {
					PublishRequest publishRequest = new PublishRequest(topic_arn, msg).withMessageAttributes(messageAttributes);
					PublishResult result = snsClient.publish(publishRequest);
					return result;
				}
			}.call();
			
		} catch (Exception e) {
			logger.error(String.format("Exhausted retries.  Publishing notification metric for failure and moving on.  Failed msg to publish: {}", msg), e);
			this.notificationMeasurement.incrementFailureCnt(1);
			return;
		}

		//If here, message was published.  As a extra validation, ensure we have a msg id
		String publishedMsgId = publishResult.getMessageId();
		if (publishedMsgId == null || publishedMsgId.isEmpty() ) {
			this.notificationMeasurement.incrementFailureCnt(1);
			return;
		}

		this.notificationMeasurement.incrementSuccessCnt(1);
		if (logger.isDebugEnabled()) {
			logger.debug("Published msg:  {} aws sns messageId - {}", msg, publishedMsgId);
		}
	}

	
}