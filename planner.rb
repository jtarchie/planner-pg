require 'pg'
require 'logger'

$logger = Logger.new(STDERR)

module Planner
  def self.conn=(conn)
    @conn = conn
  end

  def self.conn
    @conn
  end

  def self.migrate!
    conn.exec(<<~SQL)
      CREATE TYPE state AS ENUM ('failed', 'pending', 'unstarted', 'success');
      CREATE TABLE tasks (
        name TEXT NOT NULL,
        state state NOT NULL DEFAULT 'unstarted',
        index INTEGER NOT NULL
      );
      CREATE OR REPLACE FUNCTION insert_serial(n TEXT) RETURNS void AS
      $func$
        BEGIN
          INSERT INTO tasks (name, index) VALUES (n, (SELECT coalesce(max(index), 0) FROM tasks));
        END;
      $func$
      LANGUAGE plpgsql;

      CREATE OR REPLACE FUNCTION insert_parallel(n TEXT) RETURNS void AS
      $func$
        BEGIN
          INSERT INTO tasks (name, index) VALUES (n, (SELECT coalesce(max(index), 0) FROM tasks) + 1);
        END
      $func$
      LANGUAGE plpgsql;

      CREATE OR REPLACE FUNCTION get_tasks_by_group() RETURNS SETOF tasks AS
      $func$
        DECLARE
          r tasks%rowtype;
        BEGIN
          FOR r IN
            SELECT
              DISTINCT ON (index) name, state
            FROM tasks
            ORDER BY tasks.index ASC, tasks.state ASC
          LOOP
            RETURN NEXT r;
          END LOOP;
          RETURN;
        END
      $func$
      LANGUAGE plpgsql;
    SQL
  end

  class Plan
    def next(states = {})
      Planner.conn.transaction do
        states.each do |name, state|
          Planner.conn.exec('UPDATE tasks SET state = $1 WHERE name = $2', [state, name])
        end
        result = Planner.conn.exec(<<~SQL)
          SELECT
              DISTINCT ON (index) name, state
            FROM tasks
            WHERE tasks.state = 'unstarted'
            ORDER BY tasks.index ASC ;
        SQL
        debug
        debug(<<~SQL)
        SELECT
              DISTINCT ON (index) name, state
            FROM tasks
            WHERE tasks.state = 'unstarted'
            ORDER BY tasks.index ASC ;
        SQL
        return result.collect{|r| r['name']}.collect(&:to_sym)
      end
    end

    def state(states = {})
      Planner.conn.transaction do
        states.each do |name, state|
          Planner.conn.exec('UPDATE tasks SET state = $1 WHERE name = $2', [state, name])
        end
        states.each do |name, state|
          Planner.conn.exec('UPDATE tasks SET state = $1 WHERE name = $2', [state, name])
        end
        result = Planner.conn.exec(<<~SQL)
          SELECT
            state
          FROM get_tasks_by_group() AS tasks
          ORDER BY tasks.index ASC, array_position(
           array[
             'unstarted', 'pending', 'failed', 'success'
           ]::state[]
          , tasks.state)
          LIMIT 1
        SQL
        debug
        return result.first['state'].to_sym
      end
    end

    private

    def debug(sql = "SELECT * FROM tasks")
      results = Planner.conn.exec(sql)
      $logger.info "results: #{results.to_a}"
    end
  end

  class BuildPlan
    def initialize(index: 0, inc: 0, &block)
      @index = index
      @inc = inc
      self.instance_eval &block
    end

    def plan
      Plan.new
    end

    def success(&block); end
    def finally(&block); end
    def failure(&block); end
    def try(&block); end

    def serial(&block)
      SerialBuildPlan.new(&block).plan
    end

    def parallel(&block)
      ParallelBuildPlan.new(&block).plan
    end
  end

  def serial(&block)
    SerialBuildPlan.new(&block).plan
  end

  def parallel(&block)
    ParallelBuildPlan.new(inc: 1, &block).plan
  end

  class SerialBuildPlan < BuildPlan
    def task(name)
      $logger.info "name: #{name}"
      Planner.conn.exec('SELECT insert_serial($1);', [name])
    end
  end

  class ParallelBuildPlan < BuildPlan
    def task(name)
      $logger.info "name: #{name}"
      Planner.conn.exec('SELECT insert_parallel($1);', [name])
    end
  end
end
