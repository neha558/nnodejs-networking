import { Pack } from 'src/models/pack.entity';
import { Rank } from 'src/models/rank.entity';
import { MigrationInterface, QueryRunner } from 'typeorm';
import { packs } from './seeds/packs.seeds';
import { ranks } from './seeds/ranks.seeds';

export class MysqlTables1685613128838 implements MigrationInterface {
  name = 'MysqlTables1685613128838';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_networking_configs` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `name` text NOT NULL, `value` text NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_networking_packs` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `_id` int(11) NOT NULL, `name` text NOT NULL, `desc` text DEFAULT NULL, `image` text DEFAULT NULL, `price` float NOT NULL, `buyLimit` int(11) DEFAULT NULL, `totalBought` int(11) DEFAULT NULL, `totalNFTs` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      "CREATE TABLE IF NOT EXISTS `module_networking_pack_bought` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `_id` int(11) NOT NULL, `packPrice` float NOT NULL, `txHash` text DEFAULT NULL, `blockChainData` text NOT NULL, `status` varchar(50) NOT NULL DEFAULT 'paid', `userId` int(11) DEFAULT NULL, `packId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
    );
    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_networking_ranks` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `_id` int(11) NOT NULL, `name` text NOT NULL, `desc` text DEFAULT NULL, `image` text DEFAULT NULL, `color` text DEFAULT NULL, `minimumBusinessRequired` int(11) NOT NULL, `minimumStarWithRank` text NOT NULL, `matchingRatio` text NOT NULL, `directSponsorBonus` float DEFAULT NULL, `rankBonus` float DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_networking_ref_profits` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `level` int(11) NOT NULL, `incomeType` int(11) NOT NULL DEFAULT 0, `totalIncome` float NOT NULL DEFAULT 0, `profit` float NOT NULL, `percentage` float NOT NULL, `userId` int(11) DEFAULT NULL, `profitFromUserId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      "CREATE TABLE IF NOT EXISTS `module_networking_users` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `account_address` text NOT NULL, `referral_code` text NOT NULL, `referral_code_seconds` text NOT NULL, `treeDepth` int(11) NOT NULL, `node` int(11) NOT NULL, `level` int(11) NOT NULL, `individualIncome` float NOT NULL DEFAULT 0, `businessIncome` float NOT NULL DEFAULT 0, `businessIncomeFirstTeam` float NOT NULL DEFAULT 0, `businessIncomeSecondTeam` float NOT NULL DEFAULT 0, `businessIncomeFirstDeltaTeam` float NOT NULL DEFAULT 0, `businessIncomeSecondDeltaTeam` float NOT NULL DEFAULT 0, `starRecords` text NOT NULL DEFAULT '', `acceptedTerms` tinyint(4) NOT NULL DEFAULT 0, `username` text DEFAULT NULL, `email` text DEFAULT NULL, `password` text DEFAULT NULL, `token` text DEFAULT NULL, `parents` text NOT NULL, `directSponsorBonus` float NOT NULL DEFAULT 0, `teamMatchingBonus` float NOT NULL DEFAULT 0, `directMatchingBonus` float NOT NULL DEFAULT 0, `rankBonus` float NOT NULL DEFAULT 0, `infinityPoolBonus` float NOT NULL DEFAULT 0, `presidentBonus` float NOT NULL DEFAULT 0, `teamACount` int(11) NOT NULL DEFAULT 0, `teamBCount` int(11) NOT NULL DEFAULT 0, `directPartner` int(11) NOT NULL DEFAULT 0, `totalWithdrawableAmount` float NOT NULL DEFAULT 0, `referredById` int(11) DEFAULT NULL, `parentId` int(11) DEFAULT NULL, `profileId` int(11) DEFAULT NULL, `wallet_address` text DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_networking_user_activities` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `account_address` text DEFAULT NULL, `act_type` text NOT NULL, `title` text NOT NULL, `description` text NOT NULL, `userId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      "CREATE TABLE IF NOT EXISTS `module_networking_user_bonus` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `amount` float NOT NULL, `bonusType` varchar(50) NOT NULL DEFAULT 'direct', `userLevel` int(11) NOT NULL DEFAULT 0, `percentage` float NOT NULL DEFAULT 0, `userId` int(11) DEFAULT NULL, `bonusFromId` int(11) DEFAULT NULL, `packBoughtId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
    );

    await queryRunner.query(
      "CREATE TABLE IF NOT EXISTS `module_networking_user_withdrawal` ( `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `to_address` text DEFAULT NULL, `description` text NOT NULL DEFAULT '', `status` varchar(50) NOT NULL DEFAULT 'pending', `txHash` text NOT NULL DEFAULT '', `userId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_configs` ( `name` text NOT NULL, `value` text NOT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_configs_test` ( `name` text NOT NULL, `value` text NOT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_nft_airdrop_daily_report` ( `account_address` text NOT NULL, `totalNFTs` int(11) NOT NULL, `tokens` int(11) NOT NULL, `claimedBefore` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(), `status` text NOT NULL, `unique_key` text DEFAULT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `userId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_token_transactions` ( `transaction_hash` text NOT NULL, `from_address` text NOT NULL, `to_address` text NOT NULL, `block_number` text NOT NULL, `value` text NOT NULL, `status` text NOT NULL, `transaction_hash_matic` text DEFAULT NULL, `transaction_hash_deposit` text DEFAULT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_transactions` ( `amount` float NOT NULL, `systemFee` float DEFAULT 0, `account_address` text NOT NULL, `operation` text NOT NULL, `operation_details` text DEFAULT NULL, `description` text DEFAULT NULL, `blockchain_tx_id` text DEFAULT NULL, `status` text NOT NULL, `to_account_address` text DEFAULT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_users` ( `account_address` text NOT NULL, `encrypted` text NOT NULL, `status` text NOT NULL, `password` text DEFAULT NULL, `role` text, `_id` text DEFAULT NULL, `nonce` int(11) DEFAULT NULL, `wallet_address` text DEFAULT NULL, `total_usdt_balance` float NOT NULL DEFAULT 0, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_withdraw_requests` ( `amount` float NOT NULL, `account_address` text NOT NULL, `description` text DEFAULT NULL, `blockchain_tx_id` text DEFAULT NULL, `status` text NOT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL, `transactionId` int(11) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'CREATE TABLE IF NOT EXISTS `module_web3_wallet_usdt_transactions` ( `amount` float NOT NULL, `systemFee` float DEFAULT 0, `account_address` text NOT NULL, `operation` text NOT NULL, `operation_details` text DEFAULT NULL, `description` text DEFAULT NULL, `blockchain_tx_id` text DEFAULT NULL, `status` text NOT NULL, `to_account_address` text DEFAULT NULL, `transaction_hash_matic` text DEFAULT NULL, `transaction_hash_deposit` text DEFAULT NULL, `transaction_hash` text NOT NULL, `id` int(11) NOT NULL, `isActive` tinyint(4) NOT NULL DEFAULT 1, `isArchived` tinyint(4) NOT NULL DEFAULT 0, `createDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6), `createdBy` varchar(300) DEFAULT NULL, `lastChangedDateTime` timestamp(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), `lastChangedBy` varchar(300) DEFAULT NULL, `internalComment` varchar(300) DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_configs` MODIFY `name` VARCHAR (500)',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_configs` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_c8d47d5721c4421d1b2a9fc2a5` (`name`) USING HASH;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_packs` MODIFY `name` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_packs` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_024cea53bf17067e50fd30c820` (`_id`), ADD UNIQUE KEY `IDX_955afba775982129dc2c45d53c` (`name`) USING HASH;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_pack_bought` ADD PRIMARY KEY (`id`), ADD KEY `FK_19465ffd25bab2bdcc12b984d6d` (`userId`), ADD KEY `FK_e3488ce527d75608911164552ef` (`packId`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_ranks` MODIFY `name` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_ranks` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_60523b100df5d8c89703fcdd2f` (`_id`), ADD UNIQUE KEY `IDX_f2ff393775699f63161fcaea58` (`name`) USING HASH;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_ref_profits` ADD PRIMARY KEY (`id`), ADD KEY `FK_10d54279b501e7c9da4b165bb37` (`userId`), ADD KEY `FK_32d966c07fcd6fa85030f0f5daa` (`profitFromUserId`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `account_address` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `referral_code` VARCHAR (100)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `referral_code_seconds` VARCHAR (100)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `username` VARCHAR (100)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `wallet_address` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_users` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_5269103a9fb3d7601efe7d2c9f` (`account_address`) USING HASH, ADD UNIQUE KEY `IDX_cbb68c3ae02829b1c563ce0f0d` (`referral_code`) USING HASH, ADD UNIQUE KEY `IDX_36177443d1e20f43770dec5c8a` (`referral_code_seconds`) USING HASH, ADD UNIQUE KEY `IDX_057d04e9c24805ec6f2613b328` (`username`) USING HASH, ADD UNIQUE KEY `IDX_d4988c39dad63e49ea57009887` (`wallet_address`) USING HASH, ADD KEY `FK_284e3c1ff036ca8863c552f72c8` (`referredById`), ADD KEY `FK_bba18063d70a82932b89644d5f8` (`parentId`), ADD KEY `FK_5849db970aee8f3fb3c222963d4` (`profileId`);',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_activities` ADD PRIMARY KEY (`id`), ADD KEY `FK_0042cba14073b8c98d23571dbff` (`userId`);',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_bonus` ADD PRIMARY KEY (`id`), ADD KEY `FK_633a398f72ba9cbf183b4867cf9` (`userId`), ADD KEY `FK_2743277561371813d219cfe20d8` (`bonusFromId`), ADD KEY `FK_a80371de559df68169aa4509761` (`packBoughtId`);',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_withdrawal` ADD PRIMARY KEY (`id`), ADD KEY `FK_d5aab9b97fa8a050a165bb87c9f` (`userId`);',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_configs` ADD PRIMARY KEY (`id`);',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_configs_test` ADD PRIMARY KEY (`id`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_nft_airdrop_daily_report` MODIFY `unique_key` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_nft_airdrop_daily_report` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_48c3dd5f7336cf544eef85306c` (`unique_key`) USING HASH, ADD KEY `FK_7e2b4c67985b1e856998c57aa33` (`userId`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_token_transactions` MODIFY `transaction_hash` VARCHAR (255)',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_token_transactions` MODIFY `transaction_hash_matic` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_token_transactions` MODIFY `transaction_hash_deposit` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_token_transactions` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_0b9da02860b8f1c312b41e6754` (`transaction_hash`) USING HASH, ADD UNIQUE KEY `IDX_8c4e08802a4431f66ad47ef03c` (`transaction_hash_matic`) USING HASH, ADD UNIQUE KEY `IDX_5132a9f2ce4fc3a663b606cc60` (`transaction_hash_deposit`) USING HASH;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_transactions` ADD PRIMARY KEY (`id`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_users` MODIFY `wallet_address` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_users` MODIFY `_id` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_users` MODIFY `account_address` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_users` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_0a2403c4e56d3fb02b2271ca42` (`account_address`) USING HASH, ADD UNIQUE KEY `IDX_bed3d0a9c8a1989953e5d17274` (`_id`) USING HASH, ADD UNIQUE KEY `IDX_b3e47cd93e030fc37141c6e316` (`wallet_address`) USING HASH;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_withdraw_requests` MODIFY `transactionId` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_withdraw_requests` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `REL_338d1d0aad459ce7eec1953fa3` (`transactionId`);',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_usdt_transactions` MODIFY `transaction_hash_matic` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_usdt_transactions` MODIFY `transaction_hash_deposit` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_usdt_transactions` MODIFY `transaction_hash` VARCHAR (255)',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_usdt_transactions` ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `IDX_53cc794bdcf7b89656d0ecca7b` (`transaction_hash_matic`) USING HASH, ADD UNIQUE KEY `IDX_463d8885670e4b79ca11e052d5` (`transaction_hash_deposit`) USING HASH, ADD UNIQUE KEY `IDX_7f0dc1289f9135383034b3627c` (`transaction_hash`) USING HASH;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_configs` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_networking_packs` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_pack_bought` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_ranks` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_ref_profits` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_users` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_activities` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_bonus` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_withdrawal` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_configs` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_configs_test` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );

    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_nft_airdrop_daily_report` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_token_transactions` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_transactions` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_users` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_withdraw_requests` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_usdt_transactions` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_pack_bought` ADD CONSTRAINT `FK_19465ffd25bab2bdcc12b984d6d` FOREIGN KEY (`userId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_e3488ce527d75608911164552ef` FOREIGN KEY (`packId`) REFERENCES `module_networking_packs` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_ref_profits` ADD CONSTRAINT `FK_10d54279b501e7c9da4b165bb37` FOREIGN KEY (`userId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_32d966c07fcd6fa85030f0f5daa` FOREIGN KEY (`profitFromUserId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_users` ADD CONSTRAINT `FK_284e3c1ff036ca8863c552f72c8` FOREIGN KEY (`referredById`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_5849db970aee8f3fb3c222963d4` FOREIGN KEY (`profileId`) REFERENCES `module_networking_ranks` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_bba18063d70a82932b89644d5f8` FOREIGN KEY (`parentId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;    ',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_activities` ADD CONSTRAINT `FK_0042cba14073b8c98d23571dbff` FOREIGN KEY (`userId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_bonus` ADD CONSTRAINT `FK_2743277561371813d219cfe20d8` FOREIGN KEY (`bonusFromId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_633a398f72ba9cbf183b4867cf9` FOREIGN KEY (`userId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, ADD CONSTRAINT `FK_a80371de559df68169aa4509761` FOREIGN KEY (`packBoughtId`) REFERENCES `module_networking_pack_bought` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_networking_user_withdrawal` ADD CONSTRAINT `FK_d5aab9b97fa8a050a165bb87c9f` FOREIGN KEY (`userId`) REFERENCES `module_networking_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
    await queryRunner.query(
      'ALTER TABLE `module_web3_wallet_nft_airdrop_daily_report` ADD CONSTRAINT `FK_7e2b4c67985b1e856998c57aa33` FOREIGN KEY (`userId`) REFERENCES `module_web3_wallet_users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;',
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // // fifth
    await queryRunner.query(
      'DROP TABLE `module_networking_configs`, `module_networking_migration`, `module_networking_packs`, `module_networking_pack_bought`, `module_networking_ranks`, `module_networking_ref_profits`, `module_networking_users`, `module_networking_user_activities`, `module_networking_user_bonus`, `module_networking_user_withdrawal`, `module_web3_wallet_configs`, `module_web3_wallet_configs_test`, `module_web3_wallet_nft_airdrop_daily_report`, `module_web3_wallet_token_transactions`, `module_web3_wallet_transactions`, `module_web3_wallet_usdt_transactions`, `module_web3_wallet_users`, `module_web3_wallet_withdraw_requests`;',
    );
    await queryRunner.query(
      'DROP TABLE `module_networking_packs`, `module_networking_pack_bought`, `module_networking_ranks`, `module_networking_users`;',
    );
    await queryRunner.query(
      'DROP TABLE `module_networking_packs`, `module_networking_ranks`;',
    );
  }
}
