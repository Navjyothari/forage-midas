package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRecordRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    private final UserRecordRepository userRepository;
    private final TransactionRecordRepository transactionRepository;
    private final IncentiveClient incentiveClient;

    public TransactionListener(
            UserRecordRepository userRepository,
            TransactionRecordRepository transactionRepository,
            IncentiveClient incentiveClient) {

        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveClient = incentiveClient;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {

        // Find sender and recipient
        UserRecord sender = userRepository
                .findById(transaction.getSenderId())
                .orElse(null);

        UserRecord recipient = userRepository
                .findById(transaction.getRecipientId())
                .orElse(null);

        // Validate existence
        if (sender == null || recipient == null) {
            return;
        }

        // Validate sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // 🔥 CALL INCENTIVE API
        Incentive incentiveResponse = incentiveClient.getIncentive(transaction);
        float incentiveAmount = incentiveResponse.getAmount();

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount // ✅ Add incentive ONLY to recipient
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        // Save transaction WITH incentive
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);

        transactionRepository.save(record);
    }
}