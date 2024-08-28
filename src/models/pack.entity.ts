import { Entity, Column, OneToMany } from 'typeorm';
import { BaseEntity } from './base.entity';
import { PackBought } from './packBought.entity';

@Entity({ name: 'module_networking_packs' })
export class Pack extends BaseEntity {
  @Column({
    type: 'int',
    unique: true,
  })
  _id: number;

  @Column({
    type: 'text',
    unique: true,
  })
  name: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  desc: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  image: string;

  @Column({
    type: 'float',
  })
  price: number;

  @Column({
    type: 'int',
    nullable: true,
  })
  buyLimit: number;

  @Column({
    type: 'int',
    nullable: true,
  })
  totalBought: number;

  @Column({
    type: 'int',
    nullable: true,
  })
  totalNFTs: number;

  @OneToMany(() => PackBought, (packBought) => packBought.pack)
  packBought: PackBought[];
}
