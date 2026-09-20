import { Module } from '@nestjs/common';
import { ApiController } from './api.controller';
import { BackfillService } from './backfill.service';
import { CdcService } from './cdc.service';
import { ChaosService } from './chaos.service';
import { DbService } from './db.service';
import { DlqService } from './dlq.service';
import { MetricsService } from './metrics.service';
import { ReplayService } from './replay.service';
import { StatusService } from './status.service';
import { VerifyService } from './verify.service';
import { WriterService } from './writer.service';
import { EsService } from './sinks/es.service';
import { MqService } from './sinks/mq.service';

@Module({
  controllers: [ApiController],
  providers: [
    DbService,
    MetricsService,
    EsService,
    MqService,
    DlqService,
    WriterService,
    BackfillService,
    CdcService,
    ChaosService,
    ReplayService,
    StatusService,
    VerifyService,
  ],
})
export class AppModule {}
