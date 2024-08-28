import { Entity, Column } from 'typeorm';
import { BaseEntity } from './base.entity';

@Entity({ name: 'module_networking_configs' })
export class Config extends BaseEntity {
  @Column({
    type: 'text',
    unique: true,
  })
  name: string;

  @Column({
    type: 'text',
  })
  value: string;
}
