import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { BackfillService } from './backfill.service';
import { CdcService } from './cdc.service';
import { EsService } from './sinks/es.service';
import { config } from './config';
import { logger } from './logger';
import { sleep } from './util/backoff';

async function waitForEs(es: EsService): Promise<void> {
  for (let attempt = 1; attempt <= 60; attempt += 1) {
    if (await es.ping()) return;
    logger.info({ attempt }, 'waiting for elasticsearch');
    await sleep(2000);
  }
  throw new Error('elasticsearch never became reachable');
}

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { logger: false });
  app.enableCors();

  const es = app.get(EsService);
  await waitForEs(es);
  await es.ensureIndex();

  await app.listen(config.port, '0.0.0.0');
  logger.info({ port: config.port }, 'pipeline listening');

  // Resume before anyone asks. A process that comes back from SIGKILL and
  // sits idle until an operator presses start has not passed G1, whatever
  // its checkpoint says.
  const backfill = app.get(BackfillService);
  const cdc = app.get(CdcService);
  await backfill.resumeIfRunning();
  await cdc.resumeIfRunning();
}

bootstrap().catch((err: unknown) => {
  logger.error({ err: String(err) }, 'pipeline failed to start');
  process.exit(1);
});
