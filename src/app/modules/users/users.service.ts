import {
  Injectable,
  BadRequestException,
  UnauthorizedException,
  Logger,
} from '@nestjs/common';
import { User } from 'src/models/user.entity';
import { UserRegisterDTO } from './dto/UserRegisterDTO.dto';
import { DatabaseService } from 'src/app/database/database.service';
import { In, Raw } from 'typeorm';
import { BonusService } from './bonus/bonus.service';
import { VendorWalletServiceService } from 'src/app/vendor-wallet-service/vendor-wallet-service.service';

import * as crypto from 'crypto';
import { JwtService } from '@nestjs/jwt';
import { MailService } from 'src/app/mail/mail.service';
import { configService } from 'src/app/config/config.service';
import { PackBoughtStatus } from 'src/models/packBought.entity';
import { settings } from 'src/app/common/constants/general-settings';
const bcrypt = require('bcrypt');
const ethUtil = require('ethereumjs-util');

@Injectable()
export class UsersService {
  constructor(
    private readonly jwtService: JwtService,
    private readonly databaseService: DatabaseService,
    private readonly bonusService: BonusService,
    private readonly vendorWalletServiceService: VendorWalletServiceService,
    private readonly mailService: MailService,
  ) {}

  async userRepository() {
    return this.databaseService.getUserRepository();
  }

  async rankRepository() {
    return this.databaseService.getRankRepository();
  }

  async packRepository() {
    return this.databaseService.getPackRepository();
  }

  async packBoughtRepository() {
    return this.databaseService.getPackBoughtRepository();
  }

  async generateReferralCode() {
    let exists;
    let referralCode;
    do {
      referralCode = Math.floor(100000 + Math.random() * 900000).toString();
      const userRepo = await this.userRepository();
      exists = await userRepo.findOne({
        where: {
          referralCode,
        },
      });
    } while (exists);
    return referralCode;
  }

  async generateSecondReferralCode() {
    let exists;
    let referralCode;
    do {
      referralCode = Math.floor(100000 + Math.random() * 900000).toString();
      const userRepo = await this.userRepository();
      exists = await userRepo.findOne({
        where: {
          referralCodeSecond: referralCode,
        },
      });
    } while (exists);
    return referralCode;
  }

  async getUserLevel() {
    return 0;
  }

  async getUserBasedOnAccountAddress(
    accountAddress: string,
    oldImport = false,
  ) {
    if (!accountAddress) {
      return null;
    }
    const userRepo = await this.userRepository();
    const user = await userRepo.findOne({
      where: oldImport
        ? [
            {
              walletAddress: accountAddress?.toLowerCase(),
            },
            {
              accountAddress: accountAddress?.toLowerCase(),
            },
          ]
        : [
            {
              accountAddress: accountAddress?.toLowerCase(),
            },
            {
              walletAddress: accountAddress?.toLowerCase(),
            },
          ],
    });
    return user;
  }

  async acceptTerms(accountAddress: string): Promise<any> {
    const userRepo = await this.userRepository();
    const user = await userRepo.update(
      {
        accountAddress: accountAddress?.toLowerCase(),
      },
      {
        acceptedTerms: true,
      },
    );
    return user;
  }

  async login(data) {
    const userRepo = await this.userRepository();
    const exists = await userRepo.findOne({
      where: {
        email: data.email,
      },
    });

    if (!exists?.id) {
      throw new UnauthorizedException(
        'Account is not exists, Please provide valid credentials.',
      );
    }
    if (!exists?.isActive) {
      throw new UnauthorizedException(
        'Account is not active, Please check your mail and activate from your account.',
      );
    }

    if (!data?.password) {
      throw new UnauthorizedException('Not valid credentials');
    }

    if (!(await bcrypt.compare(data?.password, exists?.password))) {
      throw new UnauthorizedException('Not valid credentials');
    }

    return {
      accessToken: this.jwtService.sign(
        {
          id: exists?.id,
          accountAddress: exists?.accountAddress,
          email: exists?.email,
        },
        {
          secret: configService.getJWTSecret(),
        },
      ),
      ...exists,
      token: null,
    };
  }

  async register(
    userRegisterDTO: UserRegisterDTO | any,
    oldImport = false,
  ): Promise<User> {
    const userRepo = await this.userRepository();
    if (oldImport) {
      const alreadyAdded = await userRepo.findOne({
        where: {
          walletAddress: userRegisterDTO?.walletAddress,
        },
      });
      if (alreadyAdded?.id) {
        console.log('alreadyAdded added', alreadyAdded?.walletAddress);
        return;
      }
    }

    const exists = await userRepo.findOne({
      where: {
        email: userRegisterDTO.email,
        isActive: oldImport,
      },
    });

    if (!oldImport && exists?.id) {
      throw new BadRequestException(
        'Account exists with same email and not verified yet, Please verify and try again.',
      );
    }

    // @todo
    // userRegisterDTO.parent should be from userRegisterDTO.referredBy children only

    const [parent, referredBy] = await Promise.all([
      this.getUserBasedOnAccountAddress(userRegisterDTO.parent, oldImport),
      userRegisterDTO.referredBy
        ? userRepo.findOne({
            where: oldImport
              ? [
                  {
                    walletAddress: userRegisterDTO.referredBy?.toLowerCase(),
                  },
                  {
                    accountAddress: userRegisterDTO.referredBy?.toLowerCase(),
                  },
                ]
              : [
                  {
                    accountAddress: userRegisterDTO.referredBy?.toLowerCase(),
                  },
                  {
                    walletAddress: userRegisterDTO.referredBy?.toLowerCase(),
                  },
                ],
          })
        : Promise.resolve(null),
    ]);

    if (!parent?.id || !referredBy?.id) {
      throw new BadRequestException(
        'Parent address or referral address is not registered with us yet, Please try another one.',
      );
    }

    const userWallet =
      await this.vendorWalletServiceService.checkEmailAndAccountOfWallet(
        userRegisterDTO.email,
        userRegisterDTO.password,
        exists,
      );

    userRegisterDTO.accountAddress = userWallet?.accountAddress;

    let parents = [];
    if (parent?.id) {
      parents = [...parent?.parents, parent?.accountAddress];
    }

    let sponsorTree = [];
    if (referredBy?.id) {
      sponsorTree = [...referredBy?.sponsorTree, referredBy?.accountAddress];
    }

    let node = referredBy?.referralCodeSecond === userRegisterDTO?.code ? 1 : 0;

    if (userRegisterDTO?.team === 'teamB') {
      node = 1;
    }

    const rankRepo = await this.rankRepository();
    const defaultRank = await rankRepo.findOne({
      where: {
        _id: 1,
      },
    });

    const referralCode = await this.generateReferralCode();
    const user = await userRepo.save({
      parent,
      referredBy,
      node,
      referralCode,
      accountAddress: String(userRegisterDTO.accountAddress).toLowerCase(),
      walletAddress: userRegisterDTO.walletAddress
        ? String(userRegisterDTO.walletAddress).toLowerCase()
        : null,
      email: userRegisterDTO.email,
      userName: userRegisterDTO.userName ?? referralCode,
      treeDepth: parent?.id ? parent?.treeDepth + 1 : 0,
      acceptedTerms: false,
      referralCodeSecond: await this.generateSecondReferralCode(),
      level: await this.getUserLevel(),
      parents,
      sponsorTree,
      profile: defaultRank,
      isActive:
        configService.getByPassEmailVerification() || oldImport ? true : false,
      password:
        configService.getByPassEmailVerification() || oldImport
          ? await this.createPasswordHash(
              oldImport ? Math.random().toString() : '123456',
            )
          : undefined,
      directSponsorBonus: userRegisterDTO?.directSponsorBonus ?? 0,
      businessIncome: userRegisterDTO?.businessIncome ?? 0,
      businessIncomeFirstTeam: userRegisterDTO?.businessIncomeFirst ?? 0,
      businessIncomeSecondTeam: userRegisterDTO?.businessIncomeSecond ?? 0,
      legacyUsers: oldImport,
    });

    const token = `${crypto.randomBytes(20).toString('hex')}.${
      user?.id
    }.${new Date(user?.createDateTime).getTime()}`;

    await userRepo.update({ id: user?.id }, { token });

    // update team count of upper members as well

    // if (node === 0) {
    //   await userRepo
    //     .createQueryBuilder('networking_users')
    //     .update(User)
    //     .where(
    //       `accountAddress IN ('${[...user?.sponsorTree, ...user?.parents]?.join(
    //         `','`,
    //       )}')`,
    //     )
    //     .set({ teamACount: () => 'teamACount + :x' })
    //     .setParameter('x', 1)
    //     .execute();
    // }

    // if (node === 1) {
    //   await userRepo
    //     .createQueryBuilder('networking_users')
    //     .update(User)
    //     .where(
    //       `accountAddress IN ('${[...user?.sponsorTree, ...user?.parents]?.join(
    //         `','`,
    //       )}')`,
    //     )
    //     .set({ teamBCount: () => 'teamBCount + :x' })
    //     .setParameter('x', 1)
    //     .execute();
    // }

    if (referredBy?.id) {
      await userRepo.update(
        { id: referredBy?.id },
        {
          directPartner: referredBy?.directPartner + 1,
        },
      );
    }

    if (!oldImport) {
      // send mail to set password
      this.mailService.sendMail(
        user.email,
        'CUBIX | Password Set Request',
        `${settings.mailTemplateHTML.replace(
          '{{link}}',
          `${configService.getDomain()}/set-password/${token}`,
        )}`,
      );
    }

    return user;
  }

  async resetPassword(email: string) {
    if (!email) {
      throw new BadRequestException('Provide your email');
    }
    const userRepo = await this.userRepository();
    const user = await userRepo.findOneOrFail({
      where: {
        email,
      },
    });

    const token = `${crypto.randomBytes(20).toString('hex')}.${
      user?.id
    }.${new Date().getTime()}`;

    await userRepo.update({ id: user?.id }, { token });
    // send mail to set password
    this.mailService.sendMail(
      user.email,
      'CUBIX | Password Reset Request',
      `${settings.mailTemplateHTML.replace(
        '{{link}}',
        `${configService.getDomain()}/set-password/${token}`,
      )}`,
    );

    return true;
  }

  async setPassword(token: string, password: string) {
    if (!token || !password) {
      throw new BadRequestException('Provide password');
    }
    const userRepo = await this.userRepository();
    const user = await userRepo.findOneOrFail({
      where: {
        token,
      },
    });

    const currentTime = new Date().getTime();

    const validMinute = 10;

    if (currentTime - new Date(user?.createDateTime).getTime() < validMinute) {
      throw new UnauthorizedException(
        "You can' set the password, token is not valid",
      );
    }
    await userRepo.update(
      { token },
      {
        token: null,
        password: await this.createPasswordHash(password),
        isActive: true,
      },
    );

    await this.vendorWalletServiceService.setPasswordOfMainTable(
      user.email,
      password,
    );

    return true;
  }

  async detailsBasedOnCode(code: string): Promise<any> {
    const userRepo = await this.userRepository();

    const user = await userRepo.findOneOrFail({
      where: [
        {
          referralCode: code,
        },
        {
          referralCodeSecond: code,
        },
      ],
    });

    return {
      accountAddress: user?.accountAddress,
      team: user?.referralCode === code ? 'A' : 'B',
    };
  }

  async details(accountAddress: string, query: any): Promise<any> {
    const userRepo = await this.userRepository();
    const rankRepo = await this.rankRepository();
    const packRepo = await this.packRepository();

    const user = await userRepo.findOneOrFail({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
      relations: ['profile', 'myReferrals'],
      ...query,
    });

    const nextProfile = user?.profile?._id
      ? await rankRepo.findOne({
          where: {
            _id: user?.profile?._id + 1,
          },
        })
      : null;

    const walletProfile =
      await this.vendorWalletServiceService.getWalletUserDetails(
        user?.accountAddress,
      );

    return {
      ...user,
      nextProfile,
      walletProfile: {
        ...walletProfile,
        encrypted: null,
      },
      currentPack: await packRepo.findOne({
        where: {
          _id: user?.lastBoughtPack,
        },
      }),
      totalEarning:
        (
          user?.rankBonus +
          user?.teamMatchingBonus +
          user?.directMatchingBonus +
          user?.directSponsorBonus
        )?.toFixed?.(2) ?? '0',
    };
  }

  async buyPack(body) {
    const userRepo = await this.userRepository();
    const user = await userRepo.findOneOrFail({
      where: body?.legacyUserPack
        ? {
            walletAddress: body?.accountAddress?.toLowerCase(),
          }
        : {
            accountAddress: body?.accountAddress?.toLowerCase(),
          },
      relations: ['profile', 'referredBy', 'referredBy.profile'],
    });

    const packRepo = await this.packRepository();
    const packDetails = await packRepo.findOneOrFail({
      where: {
        _id: body?.packId,
      },
    });

    if (
      !body?.legacyUserPack &&
      !body?.addBusinessWithNFT &&
      !body?.addBusinessWithBonus
    ) {
      // check user balance from wallet service and update if needs to buy the pack
      const userWalletDetails =
        await this.vendorWalletServiceService.getWalletUserDetails(
          user.accountAddress,
        );
      if (userWalletDetails?.totalUSDTBalance < packDetails?.price) {
        throw new BadRequestException(
          "You don't have enough USDT balance in wallet, Please deposit and try again.",
        );
      }
    }

    const packBoughtRepo = await this.packBoughtRepository();

    const lastPackBought = user?.lastBoughtPack;

    if (!body?.legacyUserPack) {
      if (lastPackBought > packDetails?._id) {
        throw new BadRequestException(
          `You cant buy small pack now, Please buy same or higher amount pack.`,
        );
      }
    }

    Logger.log(
      `PACK BUY PROCESS START ${user?.accountAddress} - ${packDetails?._id}`,
    );

    const packBought = await packBoughtRepo.save({
      user,
      blockChainData: body?.blockChainData ?? '',
      pack: packDetails,
      packPrice: packDetails?.price,
      txHash: body?.txHash ?? null,
      status:
        body?.legacyUserPack || body?.addBusinessWithBonus
          ? PackBoughtStatus.nftDistributed
          : PackBoughtStatus.paid,
    });

    if (user?.lastBoughtPack !== packDetails?._id) {
      // update user isPackBought
      await userRepo.update(
        {
          id: user?.id,
        },
        {
          lastBoughtPack: packDetails?._id,
          lastBoughtPackPrice: packDetails?.price,
        },
      );
    }

    if (body?.legacyUserPack) {
      return true;
    }

    if (!body?.addBusinessWithBonus) {
      await this.vendorWalletServiceService.deductUSDTAmount(
        user.accountAddress,
        packDetails?.price,
        `${packDetails?.name} bought using ${packDetails.price} USDT from wallet`,
      );
    }

    /**
     * LOGIC for bonus and volume distribution
     * 1. Update individual income of buyer
     * 2. Update business volume of placement address as well as sponsor
     * 3. Update direct sponsor bonus for referral user
     * 4. Add Binary entries, it will have binary bonus and add on matching bonus
     * 5. Update rank related data for user and its parents
     */

    // 1. START Update individual income
    await this.bonusService.updateUserIndividualIncome(user, packBought);
    // 1. DONE Update individual income

    // 2. START Update business volume of placement address as well as sponsor
    const sponsorsAndPlacementAddressUnique = [
      ...new Set([...user?.parents, ...user?.sponsorTree]),
    ];
    const sponsorsAndPlacementAddressUniqueUsers = await userRepo.find({
      select: [
        'id',
        'accountAddress',
        'businessIncome',
        'businessIncomeFirstTeam',
        'businessIncomeSecondTeam',
        'rankBusinessATeamBucket',
        'rankBusinessBTeamBucket',
        'node',
        'teamACount',
        'teamBCount',
      ],
      where: {
        accountAddress: In(sponsorsAndPlacementAddressUnique),
      },
      order: {
        treeDepth: 'ASC',
      },
    });

    await this.bonusService.updateSponsorsIncome(
      sponsorsAndPlacementAddressUniqueUsers,
      user,
      packBought,
    );
    // 2. DONE: Update business volume of placement address as well as sponsor

    // 3. START: Update direct sponsor bonus for referral user
    await this.bonusService.updateDirectSponsorBonus(
      user,
      [user?.referredBy],
      packBought,
    );
    // 3. DONE: Update direct sponsor bonus for referral user

    // 4. START: Add Binary entries, it will have binary bonus and add on matching bonus
    const updatedSponsorTree = await userRepo.find({
      where: {
        accountAddress: In(sponsorsAndPlacementAddressUnique),
      },
      relations: ['profile', 'referredBy'],
    });

    // based on level provide team matching bonus
    // based on level provide direct team matching bonus
    await this.bonusService.updateTeamMatchingSponsorBonus(
      user,
      updatedSponsorTree,
      packBought,
    );
    // 4. DONE: Add Binary entries, it will have binary bonus and add on matching bonus

    // 5. START: Update rank related data for user and its parents
    await this.bonusService.updateUserProfileAndRankBonus(user, packBought);
    await Promise.all(
      updatedSponsorTree?.map((parent) =>
        this.bonusService.updateUserProfileAndRankBonus(parent, packBought),
      ),
    );
    // 5. DONE: Update rank related data for user and its parents

    Logger.log(
      `PACK BUY PROCESS DONE ${user?.accountAddress} - ${packDetails?._id}`,
    );
    return true;
  }

  async getParentTree(accountAddress: string) {
    const userRepo = await this.userRepository();

    const userDetails = await userRepo.findOne({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
    });

    const parents = await userRepo.find({
      where: {
        accountAddress: In(userDetails?.parents),
      },
      relations: ['profile'],
    });

    return parents;
  }

  async getSponsorTree(accountAddress: string) {
    const userRepo = await this.userRepository();

    const userDetails = await userRepo.findOne({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
    });

    const parents = await userRepo.find({
      where: {
        accountAddress: In(userDetails?.sponsorTree ?? []),
      },
      relations: ['profile'],
    });

    return parents;
  }

  async getPartners(accountAddress: string, take = '20', skip = '0') {
    const data = await this.getChildPartnersUsers(accountAddress);
    const userRepo = await this.userRepository();

    const [records, total] = await userRepo.findAndCount({
      where: {
        id: In(data?.map((d) => d.id)),
      },
      order: {
        createDateTime: 'DESC',
      },
      skip: parseInt(skip),
      take: parseInt(take),
    });

    return {
      records,
      total,
    };
  }

  async getChildPartnersUsers(accountAddress: string): Promise<Array<User>> {
    let partners = [];
    const userRepo = await this.userRepository();
    const user = await userRepo.findOne({
      select: ['id', 'accountAddress', 'createDateTime'],
      where: {
        accountAddress: Raw(
          (alias) =>
            `LOWER(${alias}) = '${String(accountAddress).toLowerCase()}'`,
        ),
      },
    });

    const children = await userRepo.find({
      select: ['id', 'accountAddress', 'createDateTime'],
      where: {
        referredBy: {
          id: user?.id,
        },
      },
    });
    partners = children;

    if (children.length <= 0) {
      return partners;
    }

    for (const child of children) {
      const data = await this.getChildPartnersUsers(child.accountAddress);
      partners = [...partners, ...data];
    }
    return partners;
  }

  async createPasswordHash(password: string): Promise<string> {
    return await bcrypt.hash(password, 10);
  }

  async getIdsBasedOnEmail(email: string): Promise<Array<User>> {
    const userRepo = await this.userRepository();
    const users = await userRepo.find({
      select: [
        'id',
        'accountAddress',
        'referralCode',
        'referralCodeSecond',
        'acceptedTerms',
        'isActive',
        'email',
      ],
      where: {
        email,
      },
    });

    return users;
  }

  async changeUserId(email: string, accountAddress: string): Promise<any> {
    const userRepo = await this.userRepository();
    const exists = await userRepo.findOne({
      where: {
        email,
        accountAddress: accountAddress?.toLowerCase(),
      },
    });

    if (!exists?.id) {
      throw new Error(
        'You cant access this ID please try login with your email.',
      );
    }
    return {
      accessToken: this.jwtService.sign(
        {
          id: exists?.id,
          accountAddress: exists?.accountAddress,
          email: exists?.email,
        },
        {
          secret: configService.getJWTSecret(),
        },
      ),
      ...exists,
      token: null,
    };
  }

  async getPartnersTree(accountAddress: string): Promise<any> {
    const userRepo = await this.userRepository();
    const user = await userRepo.findOne({
      where: {
        accountAddress,
      },
      relations: [
        'myReferrals',
        'myReferrals.myReferrals',
        'myReferrals.myReferrals.myReferrals',
        'myReferrals.myReferrals.myReferrals.myReferrals',
      ],
    });
    return user;
  }

  async setEmail(data: any) {
    await this.validateSignature(data, data?.signature);
  }

  async validateSignature(data, signature) {
    const message = `Signing with one-time nonce for wallet: ${data.nonce}`;

    const msgHex = ethUtil.bufferToHex(Buffer.from(message));
    const msgBuffer = ethUtil.toBuffer(msgHex);
    const msgHash = ethUtil.hashPersonalMessage(msgBuffer);
    const signatureBuffer = ethUtil.toBuffer(signature);
    const signatureParams = ethUtil.fromRpcSig(signatureBuffer);
    const publicKey = ethUtil.ecrecover(
      msgHash,
      signatureParams.v,
      signatureParams.r,
      signatureParams.s,
    );
    const addressBuffer = ethUtil.publicToAddress(publicKey);
    const address = ethUtil.bufferToHex(addressBuffer);

    const userRepo = await this.userRepository();

    const userExists = await userRepo.findOneOrFail({
      where: {
        walletAddress: address.toLowerCase(),
      },
    });

    if (userExists?.id) {
      const token = `${crypto.randomBytes(20).toString('hex')}.${
        userExists?.id
      }.${new Date(userExists?.lastChangedDateTime).getTime()}`;

      await userRepo.update(
        { id: userExists?.id },
        { token, email: data?.email },
      );

      this.mailService.sendMail(
        data.email,
        'CUBIX | Password Set Request',
        `${settings.mailTemplateHTML.replace(
          '{{link}}',
          `${configService.getDomain()}/set-password/${token}`,
        )}`,
      );
      return true;
    }

    throw new BadRequestException('Signature verification failed');
  }

  async changePackStatus(email, body) {
    if (email !== configService.getAdminEmail()) {
      throw new UnauthorizedException('Unauthorized');
    }
    const boughtPackRepo = await this.packBoughtRepository();
    await boughtPackRepo.update(
      {
        id: parseInt(
          body?.ids?.map((id) => id),
          10,
        ),
      },
      { status: PackBoughtStatus.initiated },
    );
    return true;
  }

  async getAccountBasedOnEmail(email: string) {
    const userRepo = await this.userRepository();
    const [records, total] = await userRepo.findAndCount({
      select: ['accountAddress', 'id', 'email'],
      where: {
        email,
      },
    });

    const accountAddressLegacy =
      await this.vendorWalletServiceService.checkEmailAndGetAccount(email);
    const data: any = records;

    if (
      accountAddressLegacy.accountAddress &&
      !records.some(
        (record) =>
          record.accountAddress?.toLowerCase() ===
          accountAddressLegacy.accountAddress,
      )
    ) {
      data.push({
        id: accountAddressLegacy.id,
        accountAddress: accountAddressLegacy.accountAddress,
        email,
      });
    }

    if (
      accountAddressLegacy.walletAddress &&
      !records.some(
        (record) =>
          record.accountAddress?.toLowerCase() ===
          accountAddressLegacy.walletAddress,
      )
    ) {
      data.push({
        id: accountAddressLegacy.id,
        accountAddress: accountAddressLegacy.walletAddress,
        email,
        isConnectedWallet: true,
      });
    }

    return {
      records: data,
      total,
    };
  }

  async updateUserTeamAAndBCount() {
    const userRepo = await this.userRepository();
    const users = await userRepo.find({
      select: ['parents', 'sponsorTree', 'node'],
    });

    const data = {};

    for (const user of users) {
      const sponsorsAndPlacementAddressUnique = [
        ...new Set([...user?.parents, ...user?.sponsorTree]),
      ];
      const sponsorsAndPlacementAddressUniqueUsers = await userRepo.find({
        select: ['id', 'accountAddress', 'email', 'referralCode', 'node'],
        where: {
          accountAddress: In(sponsorsAndPlacementAddressUnique),
        },
        order: {
          treeDepth: 'ASC',
        },
      });

      sponsorsAndPlacementAddressUniqueUsers?.forEach((sponsor, index) => {
        // based on next child decide which tree needs to be updated
        const nextChildInTree =
          sponsorsAndPlacementAddressUniqueUsers?.[index + 1] ?? user;
        // update team income
        const isFirstTeam = nextChildInTree?.node === 0;

        console.log(data?.[sponsor?.accountAddress]);

        if (data?.[sponsor?.accountAddress]) {
          if (isFirstTeam) {
            data[sponsor?.accountAddress].teamACount =
              data[sponsor?.accountAddress].teamACount + 1;
          } else {
            data[sponsor?.accountAddress].teamBCount =
              data[sponsor?.accountAddress].teamBCount + 1;
          }
        } else {
          data[sponsor?.accountAddress] = {
            id: sponsor?.id,
            accountAddress: sponsor?.accountAddress,
            email: sponsor?.email,
            referralCode: sponsor?.referralCode,
            teamACount: 1,
            teamBCount: 1,
          };
        }
        console.log(JSON.stringify(data));
      });

      // await userRepo.save(userRepoEntries);
    }

    return data;
  }

  validateSignatureOfAuthorizedPerson(data, signature) {
    const message = `Signing with one-time nonce for wallet: ${data.nonce}`;

    const msgHex = ethUtil.bufferToHex(Buffer.from(message));
    const msgBuffer = ethUtil.toBuffer(msgHex);
    const msgHash = ethUtil.hashPersonalMessage(msgBuffer);
    const signatureBuffer = ethUtil.toBuffer(signature);
    const signatureParams = ethUtil.fromRpcSig(signatureBuffer);
    const publicKey = ethUtil.ecrecover(
      msgHash,
      signatureParams.v,
      signatureParams.r,
      signatureParams.s,
    );
    const addressBuffer = ethUtil.publicToAddress(publicKey);
    const address = ethUtil.bufferToHex(addressBuffer);

    if (
      address &&
      address?.toLowerCase?.() ===
        configService.getUSDTWalletAuthorizedAddress().toLowerCase()
    ) {
      return true;
    }
    throw new BadRequestException('Signature verification failed');
  }

  async updateUSDTBackdoor(data) {
    const email = data.email;
    const accountAddress = data.accountAddress;
    const usdtBalance = data.usdtBalance;

    if (accountAddress) {
      if (email) {
        const userRepo = await this.userRepository();

        await userRepo.update(
          {
            accountAddress: accountAddress.toLowerCase(),
          },
          {
            email,
          },
        );
      }
      if (usdtBalance && parseInt(usdtBalance) > 0) {
        await this.vendorWalletServiceService.updateUSDTBalance(
          accountAddress,
          usdtBalance,
        );
      }
    }

    return true;
  }
}
