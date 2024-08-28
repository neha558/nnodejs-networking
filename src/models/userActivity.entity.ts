import { Entity, Column, ManyToOne } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';

@Entity({ name: 'module_networking_user_activities' })
export class UserActivity extends BaseEntity {
  @ManyToOne(() => User, (user) => user.userActivities)
  user: User;

  @Column({
    type: 'text',
    name: 'account_address',
    nullable: true,
  })
  accountAddress: string;

  @Column({
    type: 'text',
    name: 'act_type',
  })
  activityType: string;

  @Column({
    type: 'text',
  })
  title: string;

  @Column({
    type: 'text',
  })
  description: string;
}
