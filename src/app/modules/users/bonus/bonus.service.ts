import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PaginatedDTO } from 'src/app/common/dto/paginated.dto';
import { configService } from 'src/app/config/config.service';
import { DatabaseService } from 'src/app/database/database.service';
import { PackBought, PackBoughtStatus } from 'src/models/packBought.entity';
import { Rank } from 'src/models/rank.entity';
import { User } from 'src/models/user.entity';
import { BonusTypes } from 'src/models/userBonus.entity';
import { In, LessThanOrEqual, Raw } from 'typeorm';

const teamMatchingBonusPercentage = 7;

@Injectable()
export class BonusService {
  constructor(private readonly databaseService: DatabaseService) {}

  async userRepository() {
    return this.databaseService.getUserRepository();
  }

  async rankRepository() {
    return this.databaseService.getRankRepository();
  }

  async userBonusRepository() {
    return this.databaseService.getUserBonusRepository();
  }

  async packRepository() {
    return this.databaseService.getPackRepository();
  }

  async packBoughtRepository() {
    return this.databaseService.getPackBoughtRepository();
  }

  async userBusinessRepository() {
    return this.databaseService.getUserBusinessRepository();
  }

  async updateSponsorsIncome(sponsors: User[], user, packDetails: PackBought) {
    const userRepo = await this.userRepository();
    const userBusinessRepo = await this.userBusinessRepository();

    const userBusinessRepoEntries = sponsors?.map((sponsor, index) => {
      // based on next child decide which tree needs to be updated
      const nextChildInTree = sponsors?.[index + 1] ?? user;
      // update team income
      const isFirstTeam = nextChildInTree?.node === 0;

      return {
        node: isFirstTeam ? 0 : 1,
        accountAddress: sponsor?.accountAddress,
        businessIncome: packDetails?.packPrice,
        joinedByAccountAddress: user?.accountAddress,
      };
    });

    // traverse tree upwards and increment business income
    const userRepoEntries = sponsors?.map((sponsor, index) => {
      Logger.log(
        `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: BONUS ${sponsor?.accountAddress}: PARENT BUSINESS - ${packDetails?.packPrice}`,
      );

      // based on next child decide which tree needs to be updated
      const nextChildInTree = sponsors?.[index + 1] ?? user;
      // update team income
      const isFirstTeam = nextChildInTree?.node === 0;

      return {
        id: sponsor?.id,
        rankBusinessATeamBucket: isFirstTeam
          ? sponsor?.rankBusinessATeamBucket + packDetails?.packPrice
          : sponsor?.rankBusinessATeamBucket,
        rankBusinessBTeamBucket: !isFirstTeam
          ? sponsor?.rankBusinessBTeamBucket + packDetails?.packPrice
          : sponsor?.rankBusinessBTeamBucket,
        businessIncome: sponsor?.businessIncome + packDetails?.packPrice,
        businessIncomeFirstTeam: isFirstTeam
          ? sponsor?.businessIncomeFirstTeam + packDetails?.packPrice
          : sponsor?.businessIncomeFirstTeam,
        businessIncomeSecondTeam: !isFirstTeam
          ? sponsor?.businessIncomeSecondTeam + packDetails?.packPrice
          : sponsor?.businessIncomeSecondTeam,
        teamACount: isFirstTeam ? sponsor?.teamACount + 1 : sponsor?.teamACount,
        teamBCount: !isFirstTeam
          ? sponsor?.teamBCount + 1
          : sponsor?.teamBCount,
      };
    });

    await Promise.all([
      userBusinessRepo.save(userBusinessRepoEntries),
      userRepo.save(userRepoEntries),
    ]);
  }

  async updateUserIndividualIncome(user, packDetails: PackBought) {
    const userRepo = await this.userRepository();

    // increment user individualIncome
    await userRepo.update(
      {
        id: user?.id,
      },
      {
        individualIncome: user?.individualIncome + packDetails?.packPrice,
      },
    );

    Logger.log(
      `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: BONUS ${user?.accountAddress}: INDIVIDUAL BONUS - ${packDetails?.packPrice}`,
    );
  }

  async updateTeamMatchingSponsorBonus(
    user: User,
    sponsors: User[],
    packDetails: PackBought,
  ) {
    const userRepo = await this.userRepository();
    const bonusRepo = await this.userBonusRepository();

    const userRepoEntries = sponsors
      ?.map((sponsor) => {
        // based on level provide team direct bonus
        // based on level provide team matching direct bonus
        const firstTeamIncome =
          sponsor?.businessIncomeFirstTeam -
          sponsor?.businessIncomeFirstDeltaTeam;
        const secondTeamIncome =
          sponsor?.businessIncomeSecondTeam -
          sponsor?.businessIncomeSecondDeltaTeam;

        Logger.log(
          `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: TEAM MATCHING BONUS INCOME CHECK ${sponsor?.accountAddress}:
        firstTeamIncome: ${firstTeamIncome}
        secondTeamIncome: ${secondTeamIncome}
        `,
        );
        if (secondTeamIncome <= 0 || firstTeamIncome <= 0) {
          return null;
        }

        const eligibleIncomeForMatching =
          firstTeamIncome > secondTeamIncome
            ? secondTeamIncome
            : firstTeamIncome;

        const teamMatchingSponsorBonus =
          eligibleIncomeForMatching * (teamMatchingBonusPercentage / 100);

        let teamMatchingDirectSponsorBonus = 0;

        // not root as well
        if (
          user?.sponsorTree?.includes(sponsor?.accountAddress) &&
          sponsor?.accountAddress !==
            user?.sponsorTree?.[user?.sponsorTree.length - 1]
        ) {
          // add on matching will be not given to user who referred this sponsor
          teamMatchingDirectSponsorBonus =
            teamMatchingSponsorBonus *
            (sponsor?.profile?.directSponsorBonus / 100);
        }

        Logger.log(
          `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: TEAM MATCHING BONUS ${sponsor?.accountAddress}:
        eligibleIncomeForMatching: ${eligibleIncomeForMatching}
        teamMatchingSponsorBonus: ${teamMatchingSponsorBonus}
        teamMatchingDirectSponsorBonus: ${teamMatchingDirectSponsorBonus}
        `,
        );

        return {
          id: sponsor?.id,
          businessIncomeSecondDeltaTeam:
            sponsor?.businessIncomeSecondDeltaTeam + eligibleIncomeForMatching,
          businessIncomeFirstDeltaTeam:
            sponsor?.businessIncomeFirstDeltaTeam + eligibleIncomeForMatching,
          teamMatchingBonus:
            teamMatchingSponsorBonus > 0
              ? sponsor.teamMatchingBonus + teamMatchingSponsorBonus
              : sponsor.teamMatchingBonus,
          directMatchingBonus:
            teamMatchingDirectSponsorBonus > 0
              ? sponsor?.directMatchingBonus + teamMatchingDirectSponsorBonus
              : sponsor?.directMatchingBonus,
          teamMatchingSponsorBonus,
          teamMatchingDirectSponsorBonus,
          sponsor,
          teamMatchingBonusPercentage,
        };
      })
      .filter((_d) => _d?.id);

    const teamMatchingSponsorBonusEntries = userRepoEntries
      ?.filter((_userRepoEntries) => {
        return _userRepoEntries.teamMatchingBonus > 0;
      })
      .map((_userRepoEntry) => {
        return {
          amount: _userRepoEntry.teamMatchingSponsorBonus,
          bonusFrom: user,
          bonusType: BonusTypes.team,
          user: _userRepoEntry.sponsor,
          packBought: packDetails,
          userLevel: _userRepoEntry.sponsor?.profile?._id,
          percentage: _userRepoEntry.teamMatchingBonusPercentage,
          canClaim: false,
          flushAmount: 0,
        };
      });

    const teamMatchingDirectSponsorBonusEntries = userRepoEntries
      ?.filter((_userRepoEntries) => {
        return _userRepoEntries.teamMatchingDirectSponsorBonus > 0;
      })
      .map((_userRepoEntry) => {
        return {
          amount: _userRepoEntry.teamMatchingDirectSponsorBonus,
          bonusFrom: user,
          bonusType: BonusTypes.teamDirect,
          user: _userRepoEntry.sponsor,
          packBought: packDetails,
          userLevel: _userRepoEntry.sponsor?.profile?._id,
          percentage: _userRepoEntry.sponsor?.profile?.directSponsorBonus,
          canClaim: false,
          flushAmount: 0,
        };
      });

    await Promise.all([
      userRepo.save(userRepoEntries),
      bonusRepo.save(teamMatchingSponsorBonusEntries),
      bonusRepo.save(teamMatchingDirectSponsorBonusEntries),
    ]);
  }

  async updateDirectSponsorBonus(
    user: User,
    sponsors: User[],
    packDetails: PackBought,
  ) {
    const userRepo = await this.userRepository();

    await Promise.all(
      sponsors.map((sponsor) => {
        const directSponsorBonus =
          packDetails?.packPrice * (sponsor?.profile?.directSponsorBonus / 100);

        if (!sponsor?.id) {
          return Promise.all([Promise.resolve()]);
        }
        // check if last bought pack is _id 1 then don't provide any binary and not root user
        if (
          sponsor.referredBy &&
          (sponsor?.lastBoughtPack < 0 || sponsor?.lastBoughtPack === 1)
        ) {
          return Promise.all([Promise.resolve()]);
        }

        Logger.log(
          `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: BONUS ${sponsor?.accountAddress}: DIRECT SPONSOR BUSINESS - ${directSponsorBonus}`,
        );
        return Promise.all([
          userRepo.update(
            {
              id: sponsor?.id,
            },
            {
              directSponsorBonus:
                sponsor?.directSponsorBonus + directSponsorBonus,
            },
          ),
          this.makeEntryInBonus(
            directSponsorBonus,
            user,
            BonusTypes.direct,
            sponsor,
            packDetails,
            sponsor?.profile?.directSponsorBonus,
            sponsor?.level,
            true,
          ),
        ]);
      }),
    );
  }

  async updateUserProfileAndRankBonus(user: User, packDetails: PackBought) {
    // @todo check rank logic here
    return;
    const existingProfile = user?.profile;
    const userRepo = await this.userRepository();
    const newRank = await this.getUserCurrentLevel(user);

    if (existingProfile?._id >= newRank?.id) {
      return;
    }

    // increment user individualIncome
    const saved = await userRepo.save({
      id: user?.id,
      profile: newRank,
      rankBusinessATeamBucket:
        user?.rankBusinessATeamBucket - newRank?.minimumBusinessRequired,
      rankBusinessBTeamBucket:
        user?.rankBusinessBTeamBucket - newRank?.minimumBusinessRequired,
    });

    Logger.log(
      `U: ${user?.accountAddress} P:${packDetails?.pack?._id}: BONUS ${user?.accountAddress}: UPDATE RANK EXIST - ${saved?.profile?.id} - NEW ${newRank?.rankBonus}`,
    );

    if (saved.profile?.id !== existingProfile?.id) {
      // add rank bonus

      await userRepo.update(
        {
          id: user?.id,
        },
        {
          rankBonus: user?.rankBonus + newRank?.rankBonus,
        },
      );

      await this.makeEntryInBonus(
        newRank?.rankBonus,
        user,
        BonusTypes.rank,
        user,
        packDetails,
        0,
        user?.level,
        true,
      );
    }
  }

  async getUserCurrentLevel(userGiven: User): Promise<Rank> {
    const userRepo = await this.userRepository();

    const user = await userRepo.findOne({
      where: { id: userGiven?.id },
      relations: ['profile', 'parent'],
    });
    const rankRepo = await this.rankRepository();
    const givenRank = await rankRepo.findOne({
      where: [
        {
          minimumBusinessRequired: LessThanOrEqual(
            user?.rankBusinessATeamBucket,
          ),
        },
        {
          minimumBusinessRequired: LessThanOrEqual(
            user?.rankBusinessBTeamBucket,
          ),
        },
      ],
      order: {
        minimumBusinessRequired: 'DESC',
      },
    });

    Logger.log(
      `LEVEL LOGIC: ${user?.accountAddress} minimumBusinessRequired: A${user?.rankBusinessATeamBucket} - B${user?.rankBusinessBTeamBucket} / ${givenRank?.minimumBusinessRequired}`,
    );
    if (givenRank?.minimumBusinessRequired <= 0) {
      return user.profile;
    }

    if (givenRank?._id <= user?.profile?._id) {
      return user?.profile;
    }

    Logger.log(
      `LEVEL LOGIC: ${user?.accountAddress} directPartner: ${user?.directPartner} / ${givenRank?.directSponsorRequired}`,
    );
    // CONDITION 1: Check direct sponsor
    if (givenRank?.directSponsorRequired > user?.directPartner) {
      return user?.profile;
    }

    let canGivenRankIncrement = false;

    // CONDITION 2: Check matching
    const noOfStars = String(givenRank?.minimumStarWithRank).split('_')?.[0];
    const idOfStar = String(givenRank?.minimumStarWithRank).split('_')?.[1];

    let teamAStarCount = 0;
    let teamBStarCount = 0;

    const matchingRatio = givenRank?.matchingRatio === '2:1' ? 2 : 1;
    const teamAToBRatio =
      user?.rankBusinessBTeamBucket > 0
        ? user?.rankBusinessATeamBucket / user?.rankBusinessBTeamBucket
        : 0;
    const teamBToARatio =
      user?.rankBusinessATeamBucket > 0
        ? user?.rankBusinessBTeamBucket / user?.rankBusinessATeamBucket
        : 0;

    if (teamAToBRatio >= matchingRatio || teamBToARatio >= matchingRatio) {
      if (!givenRank?.minimumStarWithRank) {
        canGivenRankIncrement = true;
      } else {
        // find tree to get star
        const children = await this.getChildPartnersUsers(user?.accountAddress);
        teamAStarCount = children?.filter((_children) => {
          return (
            _children?.profile?._id <= parseInt(idOfStar) &&
            _children.node === 0
          );
        }).length;

        teamBStarCount = children?.filter((_children) => {
          return (
            _children?.profile?._id <= parseInt(idOfStar) &&
            _children.node === 1
          );
        }).length;

        if (matchingRatio === 1) {
          if (
            teamAStarCount >= parseInt(noOfStars, 10) &&
            teamBStarCount >= parseInt(noOfStars, 10) &&
            user?.teamACount >= 1 &&
            user?.teamBCount >= 1
          ) {
            canGivenRankIncrement = true;
          }
        }

        if (matchingRatio === 2) {
          if (
            (teamAStarCount >= parseInt(noOfStars, 10) ||
              teamBStarCount >= parseInt(noOfStars, 10)) &&
            user?.teamACount >= 1 &&
            user?.teamBCount >= 1
          ) {
            canGivenRankIncrement = true;
          }
        }
      }
    }
    // CONDITION 2: Check matching done

    if (canGivenRankIncrement) {
      Logger.log(`
        LEVEL LOGIC: ${user?.accountAddress}: ${givenRank?._id}
        DIRECT PARTNER: ${user?.directPartner}
        Team A business: ${user?.rankBusinessATeamBucket}
        Team B business: ${user?.rankBusinessBTeamBucket}
        Team A count: ${user?.teamACount}
        Team B count: ${user?.teamBCount}
        Team A star count: ${teamAStarCount}
        Team B star count: ${teamBStarCount}
        Team A to B ratio: ${teamAToBRatio}
        Team B to A ratio: ${teamBToARatio}
      `);
      return givenRank;
    }

    // other wise no update in rank
    Logger.log(`
      LEVEL LOGIC: ${user?.accountAddress}: ${user?.profile?._id}
      DIRECT PARTNER: ${user?.directPartner}
      Team A business: ${user?.rankBusinessATeamBucket}
      Team B business: ${user?.rankBusinessBTeamBucket}
      Team A count: ${user?.teamACount}
      Team B count: ${user?.teamBCount}
      Team A star count: ${teamAStarCount}
      Team B star count: ${teamBStarCount}
      Team A to B ratio: ${teamAToBRatio}
      Team B to A ratio: ${teamBToARatio}
    `);
    return user?.profile;
  }

  async makeEntryInBonus(
    amount: number,
    bonusFrom: User | null,
    bonusType: BonusTypes = BonusTypes.direct,
    user: User | null,
    packBought: PackBought,
    percentage: number,
    userLevel: number,
    canClaim,
  ) {
    const bonusRepo = await this.userBonusRepository();
    const userRepo = await this.userRepository();

    const userCurrentLevel = await userRepo.findOne({
      where: { id: user?.id },
      relations: ['profile'],
    });

    await bonusRepo.save({
      amount,
      bonusFrom,
      bonusType,
      user,
      packBought,
      userLevel: userCurrentLevel?.profile?._id,
      percentage,
      canClaim,
      flushAmount: 0,
    });

    if (canClaim) {
      // also increment user withdrawable amount
      await userRepo.update(
        {
          id: user?.id,
        },
        {
          totalWithdrawableAmount:
            (user?.totalWithdrawableAmount ?? 0) + amount,
        },
      );
    }
  }

  async getChildPartnersUsers(accountAddress: string): Promise<Array<User>> {
    let partners = [];
    const userRepo = await this.userRepository();
    const user = await userRepo.findOne({
      select: ['id', 'accountAddress', 'createDateTime'],
      relations: ['profile'],
      where: {
        accountAddress: Raw(
          (alias) =>
            `LOWER(${alias}) = '${String(accountAddress).toLowerCase()}'`,
        ),
      },
    });

    const children = await userRepo.find({
      select: ['id', 'accountAddress', 'createDateTime'],
      relations: ['profile'],
      where: {
        parent: {
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

  async getPatentTree(accountAddress: string, idOfStar: number) {
    const userRepo = await this.userRepository();

    const userDetails = await userRepo.findOne({
      select: ['id', 'parents'],
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
      relations: ['profile'],
    });

    const parents = await userRepo.find({
      where: {
        accountAddress: In(userDetails?.parents),
        profile: {
          _id: idOfStar,
        },
      },
    });

    return parents;
  }

  async getUserBonus(accountAddress, query): Promise<PaginatedDTO> {
    const bonusRepo = await this.userBonusRepository();

    const userRepo = await this.userRepository();
    const user = await userRepo.findOneOrFail({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
    });

    let where = {
      user: {
        id: user?.id,
      },
    };

    if (query?.where) {
      where = {
        ...where,
        ...query.where,
      };
    }

    const [records, total] = await bonusRepo.findAndCount({
      ...query,
      where,
    });

    return { records, total };
  }

  async getUserPacks(accountAddress, query): Promise<PaginatedDTO> {
    const userRepo = await this.userRepository();
    const packBoughtRepo = await this.packBoughtRepository();
    const user = await userRepo.findOneOrFail({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
    });

    const [records, total] = await packBoughtRepo.findAndCount({
      where: {
        user: {
          id: user?.id,
        },
      },
      ...query,
    });

    return { records, total };
  }

  async getAllUndistributedPacks(query: any): Promise<PaginatedDTO> {
    const packBoughtRepo = await this.packBoughtRepository();

    const [records, total] = await packBoughtRepo.findAndCount({
      where: {
        txHash: null,
        status: PackBoughtStatus.paid,
      },
      ...query,
    });

    return { records, total };
  }

  // every mid night
  @Cron('0 0 * * *')
  async cronForBinaryIncomeUpdate() {
    try {
      if (configService.getDisableCron()) {
        return;
      }
      const bonusRepo = await this.userBonusRepository();

      const userRepo = await this.userRepository();

      const notYetGivenBonus = await bonusRepo.find({
        where: {
          canClaim: false,
        },
        relations: ['user', 'user.profile'],
      });

      for (const _notYetGivenBonus of notYetGivenBonus) {
        if (_notYetGivenBonus?.user?.lastBoughtPack) {
          if (_notYetGivenBonus.bonusType === BonusTypes.team) {
            const lastPackPrice = _notYetGivenBonus?.user?.lastBoughtPackPrice;
            const flushAmount = _notYetGivenBonus?.amount - lastPackPrice;
            const dataToUpdateForBonus: any = {
              onHold: true,
              canClaim: true,
            };

            if (flushAmount > 0) {
              dataToUpdateForBonus.flushAmount = flushAmount;
              dataToUpdateForBonus.amount =
                _notYetGivenBonus?.amount - flushAmount;
            }

            if (
              _notYetGivenBonus?.user?.teamACount >= 1 &&
              _notYetGivenBonus?.user?.teamBCount >= 1
            ) {
              dataToUpdateForBonus.onHold = false;
            }

            Logger.log(
              `Binary BONUS UPDATES | ${
                _notYetGivenBonus?.user?.accountAddress
              } | lastPackPrice - ${
                _notYetGivenBonus?.user?.lastBoughtPackPrice
              } | flush Amount - ${flushAmount} | teamACount - ${
                _notYetGivenBonus?.user?.teamACount
              } | teamBCount - ${
                _notYetGivenBonus?.user?.teamBCount
              } | ${JSON.stringify(dataToUpdateForBonus)}`,
            );
            if (
              !dataToUpdateForBonus.onHold &&
              dataToUpdateForBonus.amount > 0
            ) {
              // increment totalWithdrawableAmount
              await userRepo.update(
                {
                  id: _notYetGivenBonus?.user?.id,
                },
                {
                  totalWithdrawableAmount:
                    (_notYetGivenBonus?.user?.totalWithdrawableAmount ?? 0) +
                    dataToUpdateForBonus.amount,
                },
              );
            }
            // update bonus entry
            await bonusRepo.update(
              { id: _notYetGivenBonus?.id },
              dataToUpdateForBonus,
            );
          }

          if (_notYetGivenBonus.bonusType === BonusTypes.teamDirect) {
            Logger.log(
              `Matching Binary BONUS UPDATES | ${_notYetGivenBonus?.user?.accountAddress} | Amount - ${_notYetGivenBonus.amount}`,
            );
            // increment totalWithdrawableAmount
            await userRepo.update(
              {
                id: _notYetGivenBonus?.user?.id,
              },
              {
                totalWithdrawableAmount:
                  (_notYetGivenBonus?.user?.totalWithdrawableAmount ?? 0) +
                  _notYetGivenBonus.amount,
              },
            );
            await bonusRepo.update(
              { id: _notYetGivenBonus?.id },
              {
                canClaim: true,
              },
            );
          }
        }
      }
    } catch (error) {
      Logger.log('Error in cron');
      console.log(error);
    }
  }
}
