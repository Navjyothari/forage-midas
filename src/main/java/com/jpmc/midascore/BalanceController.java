package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRecordRepository;
import org.springframework.web.bind.annotation.*;

@RestController
public class BalanceController {

    private final UserRecordRepository userRepository;

    public BalanceController(UserRecordRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {

        UserRecord user = userRepository.findById(userId).orElse(null);

        if (user == null) {
            return new Balance(0);
        }

        return new Balance(user.getBalance());
    }
}