import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity({ name: 'module_web3_wallet_users' })
export class ModuleWeb3WalletUser {
  @Column({
    type: 'text',
    name: 'account_address',
    unique: true,
  })
  accountAddress: string;

  @Column({
    type: 'text',
  })
  encrypted: string;

  @Column({
    type: 'text',
    default: 'pending',
  })
  status: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  password: string;

  @Column({
    type: 'text',
    nullable: true,
    default: 'user',
  })
  role: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  _id: string;

  @Column({
    type: 'int',
    nullable: true,
  })
  nonce: number;

  @Column({
    type: 'text',
    nullable: true,
    unique: true,
    name: 'wallet_address',
  })
  walletAddress: string;

  @Column({
    type: 'float',
    default: 0,
    name: 'total_usdt_balance',
  })
  totalUSDTBalance: number;

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
