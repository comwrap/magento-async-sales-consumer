<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */

declare(strict_types=1);

namespace Magento\SampleSalesConsumer\Model;

use Magento\Framework\App\ResourceConnection;
use Psr\Log\LoggerInterface;
use Magento\Framework\MessageQueue\MessageLockException;
use Magento\Framework\MessageQueue\ConnectionLostException;
use Magento\Framework\Exception\NotFoundException;
use Magento\Framework\MessageQueue\CallbackInvoker;
use Magento\Framework\MessageQueue\ConsumerConfigurationInterface;
use Magento\Framework\MessageQueue\EnvelopeInterface;
use Magento\Framework\MessageQueue\QueueInterface;
use Magento\Framework\MessageQueue\LockInterface;
use Magento\Framework\MessageQueue\MessageController;
use Magento\Framework\MessageQueue\ConsumerInterface;

use Magento\Framework\Serialize\Serializer\Json;
use Magento\AsynchronousOperations\Api\Data\OperationInterface;
use Magento\Framework\Bulk\OperationManagementInterface;
use Magento\AsynchronousOperations\Model\ConfigInterface as AsyncConfig;
use Magento\Framework\MessageQueue\MessageValidator;
use Magento\Framework\MessageQueue\MessageEncoder;
use Magento\Framework\Exception\NoSuchEntityException;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Exception\TemporaryStateExceptionInterface;
use Magento\Framework\DB\Adapter\ConnectionException;
use Magento\Framework\DB\Adapter\DeadlockException;
use Magento\Framework\DB\Adapter\LockWaitException;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\Framework\Communication\ConfigInterface as CommunicationConfig;
use Magento\Sales\Api as SalesApi;

/**
 * Merged class of MassConsumer and OperationProcessor with changes in
 * $this->processOperation
 *
 * @see \Magento\AsynchronousOperations\Model\MassConsumer
 * @see \Magento\AsynchronousOperations\Model\OperationProcessor
 */
class MassConsumerSales implements ConsumerInterface
{
    /**
     * @var \Magento\Framework\MessageQueue\CallbackInvoker
     */
    private $invoker;

    /**
     * @var \Magento\Framework\App\ResourceConnection
     */
    private $resource;

    /**
     * @var \Magento\Framework\MessageQueue\ConsumerConfigurationInterface
     */
    private $configuration;

    /**
     * @var \Magento\Framework\MessageQueue\MessageController
     */
    private $messageController;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Json
     */
    private $jsonHelper;

    /**
     * @var OperationManagementInterface
     */
    private $operationManagement;

    /**
     * @var MessageEncoder
     */
    private $messageEncoder;

    /**
     * @var MessageValidator
     */
    private $messageValidator;

    /**
     * @var ServiceOutputProcessor
     */
    private $serviceOutputProcessor;

    /**
     * @var CommunicationConfig
     */
    private $communicationConfig;

    /** @var \Magento\Sales\Model\ResourceModel\Order\CollectionFactory */
    private $orderCollection;

    /** @var \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory */
    private $orderItemCollection;

    /**
     * MassConsumerSales constructor.
     *
     * @param \Magento\Framework\MessageQueue\CallbackInvoker $invoker
     * @param \Magento\Framework\App\ResourceConnection $resource
     * @param \Magento\Framework\MessageQueue\MessageController $messageController
     * @param \Magento\Framework\MessageQueue\ConsumerConfigurationInterface $configuration
     * @param \Psr\Log\LoggerInterface $logger
     * @param \Magento\Framework\MessageQueue\MessageValidator $messageValidator
     * @param \Magento\Framework\MessageQueue\MessageEncoder $messageEncoder
     * @param \Magento\Framework\Serialize\Serializer\Json $jsonHelper
     * @param \Magento\Framework\Bulk\OperationManagementInterface $operationManagement
     * @param \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor
     * @param \Magento\Framework\Communication\ConfigInterface $communicationConfig
     * @param \Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderCollection
     * @param \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory $orderItemCollection
     */
    public function __construct(
        CallbackInvoker $invoker,
        ResourceConnection $resource,
        MessageController $messageController,
        ConsumerConfigurationInterface $configuration,
        LoggerInterface $logger,
        MessageValidator $messageValidator,
        MessageEncoder $messageEncoder,
        Json $jsonHelper,
        OperationManagementInterface $operationManagement,
        ServiceOutputProcessor $serviceOutputProcessor,
        CommunicationConfig $communicationConfig,
        \Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderCollection,
        \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory $orderItemCollection
    ) {
        $this->invoker = $invoker;
        $this->resource = $resource;
        $this->messageController = $messageController;
        $this->configuration = $configuration;

        $this->logger = $logger;
        $this->messageValidator = $messageValidator;
        $this->messageEncoder = $messageEncoder;
        $this->configuration = $configuration;
        $this->jsonHelper = $jsonHelper;
        $this->operationManagement = $operationManagement;
        $this->logger = $logger;
        $this->serviceOutputProcessor = $serviceOutputProcessor;
        $this->communicationConfig = $communicationConfig;
        $this->orderCollection = $orderCollection;
        $this->orderItemCollection = $orderItemCollection;
    }

    /**
     * {@inheritdoc}
     */
    public function process($maxNumberOfMessages = null)
    {
        $queue = $this->configuration->getQueue();

        if (!isset($maxNumberOfMessages)) {
            $queue->subscribe($this->getTransactionCallback($queue));
        } else {
            $this->invoker->invoke($queue, $maxNumberOfMessages, $this->getTransactionCallback($queue));
        }
    }

    /**
     * Get transaction callback. This handles the case of async.
     *
     * @param QueueInterface $queue
     * @return \Closure
     */
    private function getTransactionCallback(QueueInterface $queue)
    {
        return function (EnvelopeInterface $message) use ($queue) {
            /** @var LockInterface $lock */
            $lock = null;
            try {
                $topicName = $message->getProperties()['topic_name'];
                $lock = $this->messageController->lock($message, $this->configuration->getConsumerName());

                $allowedTopics = $this->configuration->getTopicNames();
                if (in_array($topicName, $allowedTopics)) {
                    $this->processOperation($message->getBody());
                } else {
                    $queue->reject($message);
                    return;
                }
                $queue->acknowledge($message);
            } catch (MessageLockException $exception) {
                $queue->acknowledge($message);
            } catch (ConnectionLostException $e) {
                if ($lock) {
                    $this->resource->getConnection()
                        ->delete($this->resource->getTableName('queue_lock'), ['id = ?' => $lock->getId()]);
                }
            } catch (NotFoundException $e) {
                $queue->acknowledge($message);
                $this->logger->warning($e->getMessage());
            } catch (\Exception $e) {
                $queue->reject($message, false, $e->getMessage());
                if ($lock) {
                    $this->resource->getConnection()
                        ->delete($this->resource->getTableName('queue_lock'), ['id = ?' => $lock->getId()]);
                }
            }
        };
    }

    /**
     * Process topic-based encoded message
     * with custom logic to get order_id and order_item_id by order_increment_id
     * so will be possible to perform BulkOrdersAdd->BulkInvoicesAdd->BulkCreditmemoAdd
     * without waiting/checking bulk status and parsing order entity_id from result_data
     *
     * @param string $encodedMessage
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function processOperation(string $encodedMessage)
    {
        $operation = $this->messageEncoder->decode(AsyncConfig::SYSTEM_TOPIC_NAME, $encodedMessage);
        $this->messageValidator->validate(AsyncConfig::SYSTEM_TOPIC_NAME, $operation);

        $status = OperationInterface::STATUS_TYPE_COMPLETE;
        $errorCode = null;
        $messages = [];
        $topicName = $operation->getTopicName();
        $handlers = $this->configuration->getHandlers($topicName);
        try {
            $data = $this->jsonHelper->unserialize($operation->getSerializedData());

            $entityParams = $this->messageEncoder->decode($topicName, $data['meta_information']);
            $entityParamsNew = [];

            //missing data resolving
            $isSalesTopic = strpos($topicName, 'async.magento.sales.api');
            $isInvoiceTopic = strpos($topicName, 'invoice');
            $isCreditmemoTopic = strpos($topicName, 'creditmemo');
            foreach ($entityParams as $entity) {

                if ($isSalesTopic !== false && ($isInvoiceTopic !== false || $isCreditmemoTopic !== false)) {
                    /** @var SalesApi\Data\InvoiceInterface|SalesApi\Data\CreditmemoInterface $entity */

                    $ea = $entity->getExtensionAttributes();
                    $orderId = $entity->getOrderId();
                    if (isset($ea) && !isset($orderId)) {
                        $incrementId = $ea->getOrderIncrementId();
                        /** @var \Magento\Sales\Model\ResourceModel\Order\Collection $collection */
                        $collection = $this->orderCollection->create();
                        $collection->addFieldToSelect([
                                                          'entity_id',
                                                          'increment_id'
                                                      ]);
                        $collection->addFieldToFilter('increment_id', ['eq' => $incrementId]);
                        $item = $collection->getFirstItem();
                        if (isset($item)) {
                            $orderId = $item['entity_id'];
                        }
                    }

                    if (!isset($orderId)) {
                        throw new \Exception('Order Id not found.');
                    }
                    $entity->setOrderId($orderId);

                    $skuList = [];
                    foreach ($entity->getItems() as $item) {
                        $skuList[] = $item->getSku();
                    }
                    $skuList = implode(',', $skuList);

                    /** @var \Magento\Sales\Model\ResourceModel\Order\Item\Collection $itemsCollection */
                    $itemsCollection = $this->orderItemCollection->create();
                    $itemsCollection->addFieldToSelect([
                                                           'item_id',
                                                           'order_id',
                                                           'sku'
                                                       ]);
                    $itemsCollection->addFieldToFilter('order_id', ['eq' => $orderId]);
                    $itemsCollection->addFieldToFilter('sku', ['in' => $skuList]);
                    $itemsData = [];
                    foreach ($itemsCollection->getItems() as $item) {
                        $itemsData[$item['sku']] = $item['item_id'];
                    }

                    $items = $entity->getItems();
                    $fixedItems = [];
                    foreach ($items as $key => $item) {
                        $sku = $item->getSku();
                        if (!array_key_exists($sku, $itemsData)) {
                            throw new \Exception('Order item not found.');
                        }

                        $item->setOrderItemId($itemsData[$sku]);
                        $fixedItems[$key] = $item;
                    }
                    if (empty($fixedItems)) {
                        throw new \Exception('Invoice items not set.');
                    }
                    $entity->setItems($fixedItems);
                }

                $entityParamsNew[] = $entity;
            }
            $entityParams = $entityParamsNew;

            $this->messageValidator->validate($topicName, $entityParams);
        } catch (\Exception $e) {
            $this->logger->error($e->getMessage());
            $status = OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED;
            $errorCode = $e->getCode();
            $messages[] = $e->getMessage();
        }

        $outputData = null;
        if ($errorCode === null) {
            foreach ($handlers as $callback) {
                $result = $this->executeHandler($callback, $entityParams);
                $status = $result['status'];
                $errorCode = $result['error_code'];
                $messages = array_merge($messages, $result['messages']);
                $outputData = $result['output_data'];
            }
        }

        if (isset($outputData)) {
            try {
                $communicationConfig = $this->communicationConfig->getTopic($topicName);
                $asyncHandler =
                    $communicationConfig[CommunicationConfig::TOPIC_HANDLERS][AsyncConfig::DEFAULT_HANDLER_NAME];
                $serviceClass = $asyncHandler[CommunicationConfig::HANDLER_TYPE];
                $serviceMethod = $asyncHandler[CommunicationConfig::HANDLER_METHOD];
                $outputData = $this->serviceOutputProcessor->process(
                    $outputData,
                    $serviceClass,
                    $serviceMethod
                );
                $outputData = $this->jsonHelper->serialize($outputData);
            } catch (\Exception $e) {
                $messages[] = $e->getMessage();
            }
        }

        $serializedData = (isset($errorCode)) ? $operation->getSerializedData() : null;
        $this->operationManagement->changeOperationStatus(
            $operation->getId(),
            $status,
            $errorCode,
            implode('; ', $messages),
            $serializedData,
            $outputData
        );
    }

    /**
     * Execute topic handler
     *
     * @param $callback
     * @param $entityParams
     * @return array
     */
    private function executeHandler($callback, $entityParams)
    {
        $result = [
            'status' => OperationInterface::STATUS_TYPE_COMPLETE,
            'error_code' => null,
            'messages' => [],
            'output_data' => null
        ];
        try {
            $result['output_data'] = call_user_func_array($callback, $entityParams);
            $result['messages'][] = sprintf('Service execution success %s::%s', get_class($callback[0]), $callback[1]);
        } catch (\Zend_Db_Adapter_Exception  $e) {
            $this->logger->critical($e->getMessage());
            if ($e instanceof LockWaitException
                || $e instanceof DeadlockException
                || $e instanceof ConnectionException
            ) {
                $result['status'] = OperationInterface::STATUS_TYPE_RETRIABLY_FAILED;
                $result['error_code'] = $e->getCode();
                $result['messages'][] = __($e->getMessage());
            } else {
                $result['status'] = OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED;
                $result['error_code'] = $e->getCode();
                $result['messages'][] =
                    __('Sorry, something went wrong during product prices update. Please see log for details.');
            }
        } catch (NoSuchEntityException $e) {
            $this->logger->error($e->getMessage());
            $result['status'] = ($e instanceof TemporaryStateExceptionInterface)
                ?
                OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED
                :
                OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED;
            $result['error_code'] = $e->getCode();
            $result['messages'][] = $e->getMessage();
        } catch (LocalizedException $e) {
            $this->logger->error($e->getMessage());
            $result['status'] = OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED;
            $result['error_code'] = $e->getCode();
            $result['messages'][] = $e->getMessage();
        } catch (\Exception $e) {
            $this->logger->error($e->getMessage());
            $result['status'] = OperationInterface::STATUS_TYPE_NOT_RETRIABLY_FAILED;
            $result['error_code'] = $e->getCode();
            $result['messages'][] = $e->getMessage();
        }
        return $result;
    }
}
