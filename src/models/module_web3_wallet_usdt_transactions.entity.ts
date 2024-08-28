import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ name: 'module_web3_wallet_usdt_transactions' })
export class ModuleWeb3USDTTransaction {
  @Column({
    type: 'float',
  })
  amount: string;

  @Column({
    type: 'float',
    default: 0,
    nullable: true,
  })
  systemFee: number;

  @Column({
    type: 'text',
    name: 'account_address',
  })
  accountAddress: string;

  @Column({
    type: 'text',
  })
  operation: string;

  @Column({
    type: 'text',
    name: 'operation_details',
    nullable: true,
  })
  operationDetails: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  description: string;

  @Column({
    type: 'text',
    nullable: true,
    name: 'blockchain_tx_id',
  })
  blockchainTxId: string;

  @Column({
    type: 'text',
    default: 'pending',
  })
  status: string;

  @Column({
    type: 'text',
    name: 'to_account_address',
    nullable: true,
  })
  toAccountAddress: string;

  @Column({
    type: 'text',
    unique: true,
    nullable: true,
    name: 'transaction_hash_matic',
  })
  transactionHashMatic: string;

  @Column({
    type: 'text',
    unique: true,
    nullable: true,
    name: 'transaction_hash_deposit',
  })
  transactionHashDeposit: string;

  @Column({
    type: 'text',
    unique: true,
    name: 'transaction_hash',
  })
  transactionHash: string;

  @PrimaryGeneratedColumn('increment')
  id: number;

  @Column({ type: 'boolean', default: true })
  isActive: boolean;

  @Column({ type: 'boolean', default: false })
  isArchived: boolean;

  @CreateDateColumn({ type: 'datetime', default: () => 'NOW' })
  createDateTime: Date;

  @Column({ type: 'varchar', length: 300, nullable: true })
  createdBy: string;

  @UpdateDateColumn({ type: 'datetime', default: () => 'NOW' })
  lastChangedDateTime: Date;

  @Column({ type: 'varchar', length: 300, nullable: true })
  lastChangedBy: string;

  @Column({ type: 'varchar', length: 300, nullable: true })
  internalComment: string | null;
}
