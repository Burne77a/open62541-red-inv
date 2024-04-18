#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>
#include <open62541/server_pubsub.h>
#include <open62541/server_config_default.h>

#include <stdlib.h>
#include <time.h>

#define PUBSUB_CONFIG_FASTPATH_FIXED_OFFSETS
#define PUBSUB_CONFIG_PUBLISH_CYCLE_MS 100
#define PUBSUB_CONFIG_FIELD_COUNT 10

//#define PUBSUB_UNICAST


UA_UInt32 valueStore[PUBSUB_CONFIG_FIELD_COUNT];

extern int ActivateRestoreOfSeqNr();


static void AddMinimalPubSubConfiguration(UA_Server * pServer,UA_NodeId *pConnectionIdentifier, UA_NodeId *pPublishedDataSetIdent)
{
    /* Add one PubSubConnection */
    UA_PubSubConnectionConfig connectionConfig;
    UA_KeyValuePair connectionOptions[1];
    memset(&connectionConfig, 0, sizeof(connectionConfig));
    connectionConfig.name = UA_STRING("UDP-UADP Connection 1");
    connectionConfig.transportProfileUri = UA_STRING("http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp");
    connectionConfig.enabled = UA_TRUE;
    /*OBS! Hardcoded IP-address of interface to be used. */
    UA_Boolean listen = UA_FALSE;
    connectionOptions[0].key = UA_QUALIFIEDNAME(0, "listen");
    UA_Variant_setScalar(&connectionOptions[0].value, &listen, &UA_TYPES[UA_TYPES_BOOLEAN]);

    connectionConfig.connectionProperties.map = connectionOptions;
    connectionConfig.connectionProperties.mapSize = 1;
#ifdef PUBSUB_UNICAST
    
    UA_NetworkAddressUrlDataType networkAddressUrl = {UA_STRING_NULL, UA_STRING("opc.udp://192.168.44.102:4840/")};
#else
    UA_NetworkAddressUrlDataType networkAddressUrl = {UA_STRING("192.168.44.101") , UA_STRING("opc.udp://224.0.0.22:4840/")};
#endif
    UA_Variant_setScalar(&connectionConfig.address, &networkAddressUrl, &UA_TYPES[UA_TYPES_NETWORKADDRESSURLDATATYPE]);
    connectionConfig.publisherIdType = UA_PUBLISHERIDTYPE_UINT16;
    connectionConfig.publisherId.uint16 = 2234;
    UA_Server_addPubSubConnection(pServer, &connectionConfig, pConnectionIdentifier);
    /* Add one PublishedDataSet */
    UA_PublishedDataSetConfig publishedDataSetConfig;
    memset(&publishedDataSetConfig, 0, sizeof(UA_PublishedDataSetConfig));
    publishedDataSetConfig.publishedDataSetType = UA_PUBSUB_DATASET_PUBLISHEDITEMS;
    publishedDataSetConfig.name = UA_STRING("Demo PDS");
    /* Add one DataSetField to the PDS */
    UA_Server_addPublishedDataSet(pServer, &publishedDataSetConfig, pPublishedDataSetIdent);
}

static int callCounter = 0;
static bool triggerDataSetError = false;
static void valueUpdateCallback(UA_Server *server, void *data) 
{
    callCounter++;
    for(int i = 0; i < PUBSUB_CONFIG_FIELD_COUNT; ++i)
        valueStore[i] = valueStore[i] + 1;
}



static UA_Server * SetupServer(UA_NodeId *pConnectionIdentifier, UA_NodeId *pPublishedDataSetIdent)
{
    UA_Server *pServer = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(pServer);
    UA_ServerConfig_setDefault(config);

    /*Add standard PubSub configuration (no difference to the std. configuration)*/
    AddMinimalPubSubConfiguration(pServer,pConnectionIdentifier,pPublishedDataSetIdent);
    return pServer;
}

static void CreateAndAddWriterGroupToServer(UA_Server *pServer,UA_NodeId connectionIdentifier,UA_NodeId *pWriterGroupIdent)
{
    UA_WriterGroupConfig writerGroupConfig;
    memset(&writerGroupConfig, 0, sizeof(UA_WriterGroupConfig));
    writerGroupConfig.name = UA_STRING("Demo WriterGroup");
    writerGroupConfig.publishingInterval = PUBSUB_CONFIG_PUBLISH_CYCLE_MS;
    writerGroupConfig.enabled = UA_FALSE;
    writerGroupConfig.writerGroupId = 100;
    writerGroupConfig.encodingMimeType = UA_PUBSUB_ENCODING_UADP;
    writerGroupConfig.messageSettings.encoding             = UA_EXTENSIONOBJECT_DECODED;
    writerGroupConfig.messageSettings.content.decoded.type = &UA_TYPES[UA_TYPES_UADPWRITERGROUPMESSAGEDATATYPE];
    UA_UadpWriterGroupMessageDataType writerGroupMessage;
    UA_UadpWriterGroupMessageDataType_init(&writerGroupMessage);
    /* Change message settings of writerGroup to send PublisherId,
     * WriterGroupId in GroupHeader and DataSetWriterId in PayloadHeader
     * of NetworkMessage */
    writerGroupMessage.networkMessageContentMask = (UA_UadpNetworkMessageContentMask) ((UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_PUBLISHERID |
                                                    (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_GROUPHEADER |
                                                    (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_WRITERGROUPID |
                                                    (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_SEQUENCENUMBER |
                                                    (UA_UadpNetworkMessageContentMask) UA_UADPNETWORKMESSAGECONTENTMASK_PAYLOADHEADER);
    writerGroupConfig.messageSettings.content.decoded.data = &writerGroupMessage;

    writerGroupConfig.rtLevel = UA_PUBSUB_RT_FIXED_SIZE;

    UA_Server_addWriterGroup(pServer, connectionIdentifier, &writerGroupConfig, pWriterGroupIdent);
    writerGroupConfig.publishingInterval = PUBSUB_CONFIG_PUBLISH_CYCLE_MS;
    UA_Server_updateWriterGroupConfig(pServer, *pWriterGroupIdent, &writerGroupConfig);
}

static void CreateDataSetWriter(UA_Server *pServer,UA_NodeId writerGroupIdent,UA_NodeId publishedDataSetIdent)
{
    UA_NodeId dataSetWriterIdent;
    UA_DataSetWriterConfig dataSetWriterConfig;
    memset(&dataSetWriterConfig, 0, sizeof(UA_DataSetWriterConfig));
    dataSetWriterConfig.name = UA_STRING("Demo DataSetWriter");
    dataSetWriterConfig.dataSetWriterId = 62541;
    dataSetWriterConfig.keyFrameCount = 1;

    UA_UadpDataSetWriterMessageDataType uadpDataSetWriterMessageDataType;
    UA_UadpDataSetWriterMessageDataType_init(&uadpDataSetWriterMessageDataType);
    uadpDataSetWriterMessageDataType.dataSetMessageContentMask = (UA_UadpDataSetMessageContentMask) UA_UADPDATASETMESSAGECONTENTMASK_SEQUENCENUMBER;
    dataSetWriterConfig.messageSettings.encoding             = UA_EXTENSIONOBJECT_DECODED;
    dataSetWriterConfig.messageSettings.content.decoded.type = &UA_TYPES[UA_TYPES_UADPDATASETWRITERMESSAGEDATATYPE];
    dataSetWriterConfig.messageSettings.content.decoded.data = &uadpDataSetWriterMessageDataType;

    /* Encode fields as RAW-Encoded */
    dataSetWriterConfig.dataSetFieldContentMask = UA_DATASETFIELDCONTENTMASK_RAWDATA;

    UA_Server_addDataSetWriter(pServer, writerGroupIdent, publishedDataSetIdent, &dataSetWriterConfig, &dataSetWriterIdent);
}

static void CreateDataSet(UA_Server *pServer,UA_NodeId publishedDataSetIdent,UA_NodeId *pDataSetFieldIdent, UA_DataValue **ppValueSource)
{
       /* Add one DataSetField with static value source to PDS */
    *ppValueSource = UA_DataValue_new();
    UA_DataSetFieldConfig dsfConfig;
    for(size_t i = 0; i < PUBSUB_CONFIG_FIELD_COUNT; i++) 
    {
        memset(&dsfConfig, 0, sizeof(UA_DataSetFieldConfig));
        /* Create Variant and configure as DataSetField source */
        valueStore[i] = (UA_UInt32) i * 1000;
        UA_Variant_setScalar(&(*ppValueSource)->value, &valueStore[i],
                             &UA_TYPES[UA_TYPES_UINT32]);
        dsfConfig.field.variable.rtValueSource.rtFieldSourceEnabled = UA_TRUE;
        dsfConfig.field.variable.rtValueSource.staticValueSource = ppValueSource;
        UA_Server_addDataSetField(pServer, publishedDataSetIdent, &dsfConfig, pDataSetFieldIdent);
    }
}

void InstallPubSubCallbacks(UA_Server *pServer,UA_NodeId writerGroupIdent)
{
    /* Freeze the PubSub configuration (and start implicitly the publish callback) */
    UA_Server_freezeWriterGroupConfiguration(pServer, writerGroupIdent);
    UA_Server_enableWriterGroup(pServer, writerGroupIdent);

    UA_UInt64 callbackId;
    UA_Server_addRepeatedCallback(pServer, valueUpdateCallback, NULL, PUBSUB_CONFIG_PUBLISH_CYCLE_MS, &callbackId);
}

static UA_Server * CreateAndSetupServerWithBasicPubSub(UA_DataValue **ppValueSource)
{
    UA_NodeId publishedDataSetIdent, dataSetFieldIdent, writerGroupIdent, connectionIdentifier;
    UA_Server *pServer = SetupServer(&connectionIdentifier,&publishedDataSetIdent);
    if(pServer == NULL)
    {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Failed to create server");
        return NULL;
    }

    CreateAndAddWriterGroupToServer(pServer,connectionIdentifier,&writerGroupIdent);
    CreateDataSetWriter(pServer,writerGroupIdent,publishedDataSetIdent);
    CreateDataSet(pServer,publishedDataSetIdent,&dataSetFieldIdent,ppValueSource);
    InstallPubSubCallbacks(pServer,writerGroupIdent);
    return pServer;
}

static void RunTheServer(UA_Server *pServer, unsigned int duration)
{
    time_t startTime = time(NULL);
    do
    {
        const UA_UInt16 waitTime = UA_Server_run_iterate(pServer, true);
    }while(difftime(time(NULL), startTime) < duration);   
    ActivateRestoreOfSeqNr();
    UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Server loop exited");
    UA_Server_run_shutdown(pServer);
}

int main(void) 
{
    UA_DataValue *pValueSource;
    UA_Server *pServerOne = CreateAndSetupServerWithBasicPubSub(&pValueSource);
    if(pServerOne)
    {
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Starting the primary");
        RunTheServer(pServerOne,20);
    }
    else
    {
         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Failed to create server one");
    
    }
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Primary failed, starting the backup");
    UA_Server *pServerTwo = CreateAndSetupServerWithBasicPubSub(&pValueSource);
    if(pServerTwo)
    {
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Starting the backup");
        RunTheServer(pServerTwo,30);
    }
    else
    {
         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Failed to create server two");
    
    }


    return 0;
}