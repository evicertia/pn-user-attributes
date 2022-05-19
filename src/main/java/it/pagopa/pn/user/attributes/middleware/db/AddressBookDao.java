package it.pagopa.pn.user.attributes.middleware.db;

import it.pagopa.pn.user.attributes.config.PnUserattributesConfig;
import it.pagopa.pn.user.attributes.exceptions.InternalErrorException;
import it.pagopa.pn.user.attributes.exceptions.NotFoundException;
import it.pagopa.pn.user.attributes.middleware.db.entities.AddressBookEntity;
import it.pagopa.pn.user.attributes.middleware.db.entities.BaseEntity;
import it.pagopa.pn.user.attributes.middleware.db.entities.VerificationCodeEntity;
import it.pagopa.pn.user.attributes.middleware.db.entities.VerifiedAddressEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Repository
@Slf4j
public class AddressBookDao extends BaseDao {

    DynamoDbAsyncTable<AddressBookEntity> addressBookTable;
    DynamoDbAsyncTable<VerificationCodeEntity> verificationCodeTable;
    DynamoDbAsyncTable<VerifiedAddressEntity> verifiedAddressTable;
    DynamoDbAsyncClient dynamoDbAsyncClient;
    DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    String table;

    public enum CHECK_RESULT {
        NOT_EXISTS,
        ALREADY_VALIDATED
    }

    public AddressBookDao(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient,
                          DynamoDbAsyncClient dynamoDbAsyncClient,
                          PnUserattributesConfig pnUserattributesConfig
                          ) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.table = pnUserattributesConfig.getDynamodbTableName();
        this.addressBookTable = dynamoDbEnhancedAsyncClient.table(table, TableSchema.fromBean(AddressBookEntity.class));
        this.verificationCodeTable = dynamoDbEnhancedAsyncClient.table(table, TableSchema.fromBean(VerificationCodeEntity.class));
        this.verifiedAddressTable = dynamoDbEnhancedAsyncClient.table(table, TableSchema.fromBean(VerifiedAddressEntity.class));
    }

    // Crea o modifica l'entity VerificationCodeEntity

    public Mono<Object> deleteAddressBook(String recipientId, String senderId, String legal, String channelType) {
        log.debug("deleteAddressBook recipientId:{} senderId:{} legalType:{} channelType:{}", recipientId, senderId, legal, channelType);
        AddressBookEntity addressBook = new AddressBookEntity(recipientId, legal, senderId, channelType);

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":pk",  AttributeValue.builder().s(addressBook.getPk()).build());

        Expression exp = Expression.builder()
                .expression(BaseEntity.COL_PK + " = :pk")
                .expressionValues(expressionValues)
                .build();

        DeleteItemEnhancedRequest delRequest = DeleteItemEnhancedRequest.builder()
                .key(getKeyBuild(addressBook.getPk(), addressBook.getSk()))
                .conditionExpression(exp)
                .build();


        return deleteVerifiedAddressIfItsLastRemained(addressBook)
                .then(Mono.fromFuture(() -> addressBookTable.deleteItem(delRequest)
                .exceptionally(throwable -> {
                    if (throwable.getCause() instanceof ConditionalCheckFailedException)
                        throw new NotFoundException();
                    else {
                        throw new InternalErrorException();
                    }
                })));
    }


    public Flux<AddressBookEntity> getAddresses(String recipientId, String senderId, String legalType) {
        log.debug("getAddresses recipientId:{} senderId:{} legalType:{}", recipientId, senderId, legalType);

        AddressBookEntity addressBook = new AddressBookEntity(recipientId, legalType, senderId, null);

        QueryEnhancedRequest qeRequest = QueryEnhancedRequest
                .builder()
                .queryConditional(QueryConditional.sortBeginsWith(getKeyBuild(addressBook.getPk(), addressBook.getSk())))
                .scanIndexForward(true)
                .build();

        return Flux.from(addressBookTable.query(qeRequest)
                        .items());
    }


    public Flux<AddressBookEntity> getAllAddressesByRecipient(String recipientId) {
        log.debug("getAllAddressesByRecipient recipientId:{}", recipientId);

        AddressBookEntity addressBook = new AddressBookEntity(recipientId, null, null, null);

        QueryEnhancedRequest qeRequest = QueryEnhancedRequest
                .builder()
                .queryConditional(QueryConditional.keyEqualTo(getKeyBuild(addressBook.getPk())))
                .scanIndexForward(true)
                .build();


        return Flux.from(addressBookTable.query(qeRequest)
                .items());

    }

    public Mono<VerificationCodeEntity> saveVerificationCode(VerificationCodeEntity entity)
    {
        log.debug("saveVerificationCode recipientId:{} channelType:{}", entity.getRecipientId(), entity.getChannelType());

        return Mono.fromFuture(() -> verificationCodeTable.updateItem(entity));
    }


    public Mono<VerificationCodeEntity> getVerificationCode(VerificationCodeEntity entity)
    {
        log.debug("getVerificationCode recipientId:{} channelType:{}", entity.getRecipientId(), entity.getChannelType());
        return Mono.fromFuture(() -> verificationCodeTable.getItem(entity));
    }

    public Mono<CHECK_RESULT> validateHashedAddress(String recipientId, String hashedAddress)
    {
        log.debug("validateHashedAddress recipientId:{} hashedAddress:{}", recipientId, hashedAddress);
        VerifiedAddressEntity verifiedAddressEntity = new VerifiedAddressEntity(recipientId, hashedAddress, "");

        QueryEnhancedRequest qeRequest = QueryEnhancedRequest
                .builder()
                .queryConditional(QueryConditional.sortBeginsWith(getKeyBuild(verifiedAddressEntity.getPk(), verifiedAddressEntity.getSk())))
                .scanIndexForward(true)
                .build();


        return Flux.from(verifiedAddressTable.query(qeRequest).items())
                .collectList()
                .map(list -> {
                    if (list.isEmpty())
                        return CHECK_RESULT.NOT_EXISTS;

                    return CHECK_RESULT.ALREADY_VALIDATED;
                });
    }

    /**
     * Inserisce o aggiorna un item di tipo AddressBookEntity e VerifiedAddress
     *
     * @param addressBook indirizzo da salvare
     * @param verifiedAddress verifiedaddress da salvare
     *
     * @return void
     */
    public Mono<Void> saveAddressBookAndVerifiedAddress(AddressBookEntity addressBook, VerifiedAddressEntity verifiedAddress) {

        log.debug("saveAddressBookAndVerifiedAddress recipientId:{} channeltype:{} senderId:{} hashedaddress:{}",addressBook.getRecipientId(),addressBook.getChannelType(), addressBook.getSenderId(), verifiedAddress.getHashedAddress());
        TransactUpdateItemEnhancedRequest <AddressBookEntity> updRequest = TransactUpdateItemEnhancedRequest.builder(AddressBookEntity.class)
                .item(addressBook)
                .build();
        TransactUpdateItemEnhancedRequest<VerifiedAddressEntity> updVARequest = TransactUpdateItemEnhancedRequest.builder(VerifiedAddressEntity.class)
                .item(verifiedAddress)
                .build();

        TransactWriteItemsEnhancedRequest transactWriteItemsEnhancedRequest = TransactWriteItemsEnhancedRequest.builder()
                .addUpdateItem(addressBookTable, updRequest)
                .addUpdateItem(verifiedAddressTable, updVARequest)
                .build();

        return deleteVerifiedAddressIfItsLastRemained(addressBook)
                .then(Mono.fromFuture(() -> dynamoDbEnhancedAsyncClient.transactWriteItems(transactWriteItemsEnhancedRequest)));
    }

    /**
     * Elimina l'eventuale verified address se è l'ultimo rimasto e viene rimosso (o modificato) l'addressbook
     *
     * @param addressBook entity da cui partire per la ricerca
     * @return nd
     */
    private Mono<Void> deleteVerifiedAddressIfItsLastRemained(AddressBookEntity addressBook)
    {
        // step1: cerco se esiste un precedente addressbook con gli stessi parametri del mio (ignorando l'hashedaddress CORRENTE,
        // che potrebbe essere cambiato, ad esempio nel caso in cui sto sovrascrivendo, che è un caso speciale di DELETE-INSERT visto che è unico
        // step2: se lo trovo, uso il suo hashedaddress per la successiva ricerca. Se non lo trovo vuol dire che sto inserendo un nuovo indirizzo, e quindi non c'è nulla da eliminare.
        // step3: nel caso in cui esisteva un indirizzo, devo controllare se ci sono ALTRI indirizzi con lo stesso hashedaddress.
        //        Se si, non devo fare nulla (non è l'ultimo).
        //        Altrimenti, devo eliminare il verifiedaddress associato al channelType

        //NB: gli step li eseguo comunque in memoria, perchè così eseguo una richiesta unica a dynamo e non si suppone siano molti indirizzi
        return this.getAllAddressesByRecipient(addressBook.getRecipientId()).collectList().flatMap(list -> {
            log.info("deleteVerifiedAddressIfItsLastRemained there are {} for recipientId:{}", list.size(), addressBook.getRecipientId());
            // step 1
            AtomicReference<String> hashedAddressToCheck = new AtomicReference<>();
               list.forEach(ab -> {
                   if (ab.getSk().equals(addressBook.getSk()))
                   {
                       hashedAddressToCheck.set(ab.getAddresshash());
                   }
               });
            // step 2
           if (hashedAddressToCheck.get() != null)
           {
               AtomicReference<Integer> count = new AtomicReference<>(0);
               list.forEach(ab -> {
                   if (ab.getAddresshash() != null
                       && ab.getAddresshash().equals(hashedAddressToCheck.get())
                       && ab.getChannelType().equals(addressBook.getChannelType()))
                   {
                       count.set(count.get() +1);
                   }
               });
               // step 3
               if (count.get() == 1)
               {
                   log.info("deleteVerifiedAddressIfItsLastRemained this was the last one address for hash:{} and channelType:{}, removing verifiedaddress", hashedAddressToCheck.get(), addressBook.getChannelType());
                   VerifiedAddressEntity verifiedAddressToDelete =new VerifiedAddressEntity(addressBook.getRecipientId(), hashedAddressToCheck.get(), addressBook.getChannelType());
                   return Mono.fromFuture(this.verifiedAddressTable.deleteItem(verifiedAddressToDelete)).then();
               }
               else
                   log.info("deleteVerifiedAddressIfItsLastRemained there are more than one address for hash:{} and channelType:{}", hashedAddressToCheck.get(), addressBook.getChannelType());
           }
           else
               log.info("deleteVerifiedAddressIfItsLastRemained there aren't previous addresses for recipient and channeltype recipientId:{} and channelType:{}, nothing to remove", addressBook.getRecipientId(), addressBook.getChannelType());

           return Mono.empty();
        });
    }

}
