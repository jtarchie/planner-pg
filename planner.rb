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
    SQL
  end

  class Plan
    def next(states = {})
      states.each do |name, state|
        Planner.conn.exec('UPDATE tasks SET state = $1 WHERE name = $2', [state, name])
      end
      result = Planner.conn.exec(<<~SQL)
        WITH pending_tasks AS (
          SELECT
            DISTINCT ON (index) name, state
          FROM tasks
          WHERE state IN ('unstarted', 'pending', 'failed')
          ORDER BY tasks.index ASC, tasks.state ASC
        )
        SELECT name FROM pending_tasks WHERE state = 'unstarted';
      SQL
      result.collect{|r| r['name']}.collect(&:to_sym)
    end

    def state(states = {})
      states.each do |name, state|
        Planner.conn.exec('UPDATE tasks SET state = $1 WHERE name = $2', [state, name])
      end
      result = Planner.conn.exec(<<~SQL)
        SELECT
          state
        FROM tasks
        ORDER BY tasks.index ASC, tasks.state ASC
        LIMIT 1
      SQL
      result.first['state'].to_sym
    end
  end

  class BuildPlan
    def initialize(index: 0, inc: 0, &block)
      @index = index
      @inc = inc
      self.instance_eval &block
    end

    def task(name)
      @index+=@inc
      $logger.info "name: #{name}, index: #{@index}"
      Planner.conn.exec('INSERT INTO tasks (name, index) VALUES ($1, $2)', [name, @index])
    end

    def plan
      Plan.new
    end

    def success(&block); end
    def finally(&block); end
    def failure(&block); end
    def try(&block); end
    def serial(&block)
      BuildPlan.new(index: @index, &block).plan
    end
    def parallel(&block)
      BuildPlan.new(index: @index+=1, inc: 1,  &block).plan
    end
  end

  def serial(&block)
    BuildPlan.new(&block).plan
  end

  def parallel(&block)
    BuildPlan.new(inc: 1, &block).plan
  end
end
