<?php

namespace Eventio\BBQ\Queue;

use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use Eventio\BBQ\Job\AmazonSQSQueueJob;
use Eventio\BBQ\Job\JobInterface;
use Eventio\BBQ\Job\Payload\JobPayloadInterface;

/**
 * @author Danny Ritterman
 */
class AmazonSQSQueue extends AbstractQueue
{
	protected $sqs;
	protected $queueName;
	protected $queueUrl;

	public function __construct( $queueName, SqsClient $sqs, array $config = array() )
	{
		parent::__construct( $queueName, $config );

		$this->sqs       = $sqs;
		$this->queueName = $queueName;
		$this->queueUrl  = $this->queueUrl();
	}

	protected function init() { }

	/**
	 * @return bool
	 */
	public function mayHaveJob()
	{
		return true;
	}

	/**
	 * @param JobPayloadInterface $jobPayload
	 */
	public function pushJob( JobPayloadInterface $jobPayload )
	{
		$this->sqs->sendMessage(
			array(
				'QueueUrl'    => $this->queueUrl,
				'MessageBody' => $this->toMessageBody( $jobPayload )
			)
		);
	}

	/**
	 * @param JobPayloadInterface[] $batch
	 */
	public function batchPush( array $batch = array() )
	{
		$entries = array();
		foreach ( $batch as $id => $jobPayload )
		{
			$entries[ ] = array(
				'Id'          => (string) $id,
				'MessageBody' => $this->toMessageBody( $jobPayload )
			);
		}

		$this->sqs->sendMessageBatch(
			array(
				'QueueUrl' => $this->queueUrl,
				'Entries'  => $entries
			)
		);
	}

	/**
	 * @param null $timeout
	 * @return AmazonSQSQueueJob|null
	 */
	public function fetchJob( $timeout = null )
	{
		foreach ( $this->receiveMessages( 1, $timeout ) as $message )
			return $this->messageToJob( $message );

		return null;
	}

	/**
	 * SQS Only allows 10 messages to be received at a time
	 * @param $numJobs
	 * @param $timeout
	 * @return array|null
	 */
	public function batchFetch( $numJobs, $timeout )
	{
		$jobs = array();

		foreach ( $this->receiveMessages( $numJobs, $timeout ) as $message )
			$jobs[ ] = $this->messageToJob( $message );

		if ( !count( $jobs ) )
			return null;

		return $jobs;
	}

	/**
	 * @param AmazonSQSQueueJob|JobInterface $job
	 */
	public function finalizeJob( JobInterface $job )
	{
		$this->sqs->deleteMessage(
			array(
				'QueueUrl'      => $this->queueUrl,
				'ReceiptHandle' => $job->sqsReceiptHandle()
			)
		);
	}

	/**
	 * SQS Only allows 10 messages to be deleted at a time
	 * @param AmazonSQSQueueJob[] $jobs
	 */
	function batchFinalize( array $jobs = array() )
	{
		$entries = array();

		foreach ( $jobs as $id => $job )
		{
			$entries[ ] = array(
				'Id'            => (string) $id,
				'ReceiptHandle' => $job->sqsReceiptHandle()
			);
		}

		$this->sqs->deleteMessageBatch(
			array(
				'QueueUrl' => $this->queueUrl,
				'Entries'  => $entries
			)
		);
	}

	/**
	 * @param AmazonSQSQueueJob|JobInterface $job
	 */
	public function releaseJob( JobInterface $job )
	{
		$this->sqs->changeMessageVisibility(
			array(
				'QueueUrl'          => $this->queueUrl,
				'ReceiptHandle'     => $job->sqsReceiptHandle(),
				'VisibilityTimeout' => 0,
			)
		);
	}

	/**
	 * SQS Only allows 10 messages to be received at a time
	 * @param      $numMessages
	 * @param null $timeout
	 * @return array|null
	 */
	private function receiveMessages( $numMessages, $timeout = null )
	{
		if ( $numMessages < 0 )
			$numMessages = 1;

		if ( $numMessages > 10 )
			$numMessages = 10;

		$args = array(
			'QueueUrl'            => $this->queueUrl,
			'MaxNumberOfMessages' => $numMessages,
		);

		if ( isset( $timeout ) )
			$args[ 'WaitTimeSeconds' ] = $timeout;

		$result = $this->sqs->receiveMessage( $args );

		return $result->getPath( 'Messages' );
	}

	/**
	 * If no queue exists then getQueueUrl will throw an exception.
	 * Catch that exception and create a queue instead
	 */
	private function queueUrl()
	{
		try
		{
			$result = $this->sqs->getQueueUrl(
				array(
					'QueueName' => $this->queueName
				)
			);
		}
		catch ( SqsException $e )
		{
			$result = $this->sqs->createQueue( array(
				'QueueName' => $this->queueName
			) );
		}

		return $result->get( 'QueueUrl' );
	}

	/**
	 * @param $jobPayload
	 * @return string
	 */
	private function toMessageBody( $jobPayload )
	{
		return serialize( $jobPayload );
	}

	/**
	 * @param $message
	 * @return AmazonSQSQueueJob
	 */
	private function messageToJob( $message )
	{
		$job = new AmazonSQSQueueJob( $message );
		$job->setQueue( $this );

		return $job;
	}
}