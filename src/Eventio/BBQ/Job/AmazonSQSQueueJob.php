<?php

namespace Eventio\BBQ\Job;

/**
 * @author Danny Ritterman
 */
class AmazonSQSQueueJob extends Job
{
	private $MessageId;
	private $ReceiptHandle;

	function __construct( $amazonSqsMessage )
    {
	    $this->setPayload( unserialize( array_get( $amazonSqsMessage, 'Body', '' ) ) );
	    $this->MessageId     = array_get( $amazonSqsMessage, 'MessageId' );
	    $this->ReceiptHandle = array_get( $amazonSqsMessage, 'ReceiptHandle' );
    }

	function sqsMessageId()
	{
		return $this->MessageId;
	}

	function sqsReceiptHandle()
	{
		return $this->ReceiptHandle;
	}
}