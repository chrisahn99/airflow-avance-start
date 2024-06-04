def _find_zombies(self) -> None:
        """
        Find zombie task instances and create a TaskCallbackRequest to be handled by the DAG processor.

        Zombie instances are tasks haven't heartbeated for too long or have a no-longer-running LocalTaskJob.
        """
        from airflow.jobs.job import Job

        self.log.debug("Finding 'running' jobs without a recent heartbeat")
        limit_dttm = timezone.utcnow() - timedelta(seconds=self._zombie_threshold_secs)

        with create_session() as session:
            zombies: list[tuple[TI, str, str]] = (
                session.execute(
                    select(TI, DM.fileloc, DM.processor_subdir)
                    .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                    .join(Job, TI.job_id == Job.id)
                    .join(DM, TI.dag_id == DM.dag_id)
                    .where(TI.state == TaskInstanceState.RUNNING)
                    .where(
                        or_(
                            Job.state != JobState.RUNNING,
                            Job.latest_heartbeat < limit_dttm,
                        )
                    )
                    .where(Job.job_type == "LocalTaskJob")
                    .where(TI.queued_by_job_id == self.job.id)
                )
                .unique()
                .all()
            )

        if zombies:
            self.log.warning("Failing (%s) jobs without heartbeat after %s", len(zombies), limit_dttm)

        for ti, file_loc, processor_subdir in zombies:
            zombie_message_details = self._generate_zombie_message_details(ti)
            request = TaskCallbackRequest(
                full_filepath=file_loc,
                processor_subdir=processor_subdir,
                simple_task_instance=SimpleTaskInstance.from_ti(ti),
                msg=str(zombie_message_details),
            )
            log_message = (
                f"Detected zombie job: {request} "
                "(See https://airflow.apache.org/docs/apache-airflow/"
                "stable/core-concepts/tasks.html#zombie-undead-tasks)"
            )
            self._task_context_logger.error(log_message, ti=ti)
            self.job.executor.send_callback(request)
            Stats.incr("zombies_killed", tags={"dag_id": ti.dag_id, "task_id": ti.task_id})