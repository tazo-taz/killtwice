import { Body, Controller, Get, Header, Post, Query, Res } from '@nestjs/common';
import type { Response } from 'express';
import { config } from './config';
import { BackfillService } from './backfill.service';
import { CdcService } from './cdc.service';
import { ChaosService } from './chaos.service';
import { DlqService } from './dlq.service';
import { EsService } from './sinks/es.service';
import { MetricsService } from './metrics.service';
import { ReplayService } from './replay.service';
import { StatusService } from './status.service';
import { VerifyService } from './verify.service';

/**
 * Everything the UI can do is here, because `make verify` drives the API and
 * never the browser. A control that exists only as a button is a control the
 * gates cannot exercise.
 */
@Controller()
export class ApiController {
  constructor(
    private readonly backfill: BackfillService,
    private readonly cdc: CdcService,
    private readonly chaos: ChaosService,
    private readonly dlq: DlqService,
    private readonly es: EsService,
    private readonly metrics: MetricsService,
    private readonly replayer: ReplayService,
    private readonly statusSvc: StatusService,
    private readonly verify: VerifyService,
  ) {}

  @Get('health')
  health(): { status: string } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(@Res({ passthrough: true }) res: Response): Promise<{ ready: boolean }> {
    const status = await this.statusSvc.status();
    if (!status.healthy) res.status(503);
    return { ready: status.healthy };
  }

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metricsEndpoint(): Promise<string> {
    return this.metrics.metrics();
  }

  @Get('api/status')
  status(): Promise<unknown> {
    return this.statusSvc.status();
  }

  // ---------------------------------------------------------------- control

  @Post('api/backfill/start')
  start(@Body() body: { chunkSize?: number }): Promise<unknown> {
    return this.backfill.start(body?.chunkSize);
  }

  @Post('api/backfill/stop')
  async stopBackfill(): Promise<{ stopped: boolean }> {
    await this.backfill.stop();
    return { stopped: true };
  }

  @Post('api/stream/start')
  async startStream(): Promise<{ started: boolean }> {
    await this.cdc.start();
    return { started: true };
  }

  @Post('api/stream/stop')
  async stopStream(): Promise<{ stopped: boolean }> {
    await this.cdc.stop();
    return { stopped: true };
  }

  @Post('api/reset')
  async reset(): Promise<{ reset: boolean }> {
    await this.statusSvc.reset();
    return { reset: true };
  }

  // -------------------------------------------------------------------- dlq

  @Get('api/dlq')
  list(@Query('status') status = 'pending', @Query('limit') limit = '100'): Promise<unknown> {
    return this.dlq.list(status, Number(limit) || 100);
  }

  @Post('api/dlq/replay')
  replay(
    @Body() body: { ids?: string[]; from?: 'payload' | 'source'; limit?: number },
  ): Promise<unknown> {
    return this.replayer.replay(body?.ids, body?.from ?? 'payload', body?.limit ?? 500);
  }

  // ------------------------------------------------------------------ chaos

  @Post('api/chaos/poison')
  poison(@Body() body: { total?: number; poison?: number }): Promise<unknown> {
    return this.chaos.poison(body?.total ?? 500, body?.poison ?? 3);
  }

  @Post('api/chaos/unpoison')
  async unpoison(@Body() body: { ids?: number[] }): Promise<{ fixed: number }> {
    return { fixed: await this.chaos.unpoison(body?.ids) };
  }

  @Post('api/chaos/mutate')
  async mutate(
    @Body() body: { count?: number; op?: 'update' | 'delete' | 'mixed' },
  ): Promise<{ mutated: number }> {
    return { mutated: await this.chaos.mutate(body?.count ?? 1000, body?.op ?? 'mixed') };
  }

  @Post('api/chaos/hard-delete')
  async hardDelete(@Body() body: { count?: number }): Promise<{ deleted: number }> {
    return { deleted: await this.chaos.hardDelete(body?.count ?? 10) };
  }

  // ----------------------------------------------------------- verification

  @Get('api/verify/counts')
  counts(): Promise<unknown> {
    return this.verify.counts();
  }

  @Get('api/verify/sample')
  sample(@Query('n') n = '1000'): Promise<unknown> {
    return this.verify.sample(Math.min(Number(n) || 1000, 5000));
  }

  // ------------------------------------------------------------------- data

  @Get('api/records')
  async search(
    @Query('q') q = '',
    @Query('size') size = '25',
    @Query('from') from = '0',
  ): Promise<unknown> {
    const res = await this.es.client.search({
      index: config.esIndex,
      size: Math.min(Number(size) || 25, 100),
      from: Number(from) || 0,
      sort: [{ row_version: 'desc' }],
      // Without this Elasticsearch stops counting at 10,000 and reports
      // exactly that, so the console would show a capped number as though it
      // were the real one. An operator comparing "matching documents"
      // against the source count needs the true figure.
      track_total_hits: true,
      // phrase_prefix only works on `text` fields. Of the four searchable
      // fields only full_name is text - email, country and status are
      // keyword, and a phrase_prefix against them is a hard 400, not a
      // degraded result. Keyword fields get a `prefix` query instead.
      //
      // The case folding is tailored to the seeded data (country codes are
      // upper, everything else lower) because a keyword prefix is matched
      // verbatim. A normalised lowercase sub-field in the mapping would be
      // the general fix and would need a reindex.
      query: q
        ? {
            bool: {
              minimum_should_match: 1,
              should: [
                { match_phrase_prefix: { full_name: q } },
                { prefix: { email: q.toLowerCase() } },
                { prefix: { status: q.toLowerCase() } },
                { prefix: { country: q.toUpperCase() } },
              ],
            },
          }
        : { match_all: {} },
    });
    return {
      total: typeof res.hits.total === 'number' ? res.hits.total : res.hits.total?.value,
      hits: res.hits.hits.map((h) => h._source),
    };
  }
}
