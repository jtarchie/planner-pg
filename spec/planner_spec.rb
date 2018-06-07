# frozen_string_literal: true

require 'spec_helper'
require 'pg'

RSpec.describe 'Planner' do
  include Planner

  def conn
    count = 0
    begin
      PG.connect(
        dbname: 'testing',
        user: 'postgres',
        host: 'localhost'
      )
    rescue PG::ConnectionBad
      count += 1
      if count < 10
        sleep 0.5
        retry
      end
      raise
    end
  end

  before(:all) do
    system('docker kill database')
    system('docker rm -f database')
    expect(system('docker run -p 5432:5432 -e POSTGRES_DB=testing -d --name database postgres')).to be_truthy
    Planner.conn = conn
    Planner.migrate!
  end

  before(:each) do
    Planner.conn.exec('TRUNCATE tasks')
  end

  context 'with a single step' do
    shared_examples 'single step plan' do
      it 'returns the step when it has not executed' do
        steps = plan.next
        expect(steps).to eq [:A]
      end

      it 'has an ininstal unstarted state' do
        expect(plan.state).to eq :unstarted
      end

      it 'returns nothing once it pending' do
        steps = plan.next(A: :pending)
        expect(steps).to be_empty
      end

      it 'returns nothing once it success' do
        steps = plan.next(A: :success)
        expect(steps).to be_empty
      end

      it 'returns nothing once it failed' do
        steps = plan.next(A: :failed)
        expect(steps).to be_empty
      end
    end

    context 'in serial' do
      let(:plan) do
        serial do
          task :A
        end
      end
      it_behaves_like 'single step plan'
    end

    context 'in parallel' do
      let(:plan) do
        parallel do
          task :A
        end
      end
      it_behaves_like 'single step plan'
    end
  end

  context 'with two serial steps' do
    shared_examples 'multiple serial steps plan' do
      it 'returns the first step when it has not executed' do
        steps = plan.next
        expect(steps).to eq [:A]
      end

      it 'has an initial unstarted state' do
        expect(plan.state).to eq :unstarted
      end

      it 'returns nothing when the first step is pending' do
        expect(plan.next(A: :pending)).to be_empty
      end

      it 'returns the next step on first success' do
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.state(A: :success)).to eq :unstarted
      end

      it 'returns nothing when the first step has failed' do
        expect(plan.next(A: :failed)).to be_empty
        expect(plan.state(A: :failed)).to eq :failed
      end

      context 'when the final step has finished' do
        it 'returns no steps' do
          expect(plan.next(A: :success, B: :success)).to eq []
          expect(plan.state(A: :success, B: :success)).to eq :success
        end
      end
    end

    context 'with two tasks' do
      let(:plan) do
        serial do
          task :A
          task :B
        end
      end
      it_behaves_like 'multiple serial steps plan'
    end

    context 'with a nested serial step' do
      context 'first is serial' do
        let(:plan) { serial { serial { task :A }; task :B } }
        it_behaves_like 'multiple serial steps plan'
      end

      context 'second is serial' do
        let(:plan) { serial { task :A; serial { task :B } } }
        it_behaves_like 'multiple serial steps plan'
      end

      context 'both are serial' do
        let(:plan) { serial { serial { task :A }; serial { task :B } } }
        it_behaves_like 'multiple serial steps plan'
      end

      context 'with a nested serials' do
        let(:plan) { serial { serial { serial { task :A; task :B } } } }
        it_behaves_like 'multiple serial steps plan'
      end
    end
  end

  context 'with two steps in parallel' do
    shared_examples 'multiple steps in parallel' do
      it 'returns all steps with no state' do
        steps = plan.next
        expect(steps).to eq %i[A B]
      end

      it 'has an initial unstarted state' do
        expect(plan.state).to eq :unstarted
      end

      it 'returns the other step when one is pending' do
        expect(plan.next(A: :pending)).to eq [:B]
        expect(plan.state(A: :pending)).to eq :unstarted

        expect(plan.next(B: :pending)).to eq [:A]
        expect(plan.state(B: :pending)).to eq :unstarted
      end

      it 'returns the other step when one is successful' do
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.state(A: :success)).to eq :unstarted

        expect(plan.next(B: :success)).to eq [:A]
        expect(plan.state(B: :success)).to eq :unstarted
      end

      it 'returns the other step when one has failed' do
        expect(plan.next(A: :failed)).to eq [:B]
        expect(plan.state(A: :failed)).to eq :unstarted

        expect(plan.next(B: :failed)).to eq [:A]
        expect(plan.state(B: :failed)).to eq :unstarted
      end

      context 'when one steps fails and one is successful' do
        it 'returns no steps' do
          expect(plan.next(A: :failed, B: :success)).to be_empty
          expect(plan.next(A: :success, B: :failed)).to be_empty
        end

        it 'has failed' do
          expect(plan.state(A: :failed, B: :success)).to eq :failed
          expect(plan.state(A: :success, B: :failed)).to eq :failed
        end
      end

      context 'when both steps fail' do
        it 'returns no steps' do
          expect(plan.next(A: :failed, B: :failed)).to be_empty
        end

        it 'has failed' do
          expect(plan.state(A: :failed, B: :failed)).to eq :failed
        end
      end

      context 'when both steps are successful' do
        it 'returns no steps' do
          expect(plan.next(A: :success, B: :success)).to be_empty
        end

        it 'has failed' do
          expect(plan.state(A: :success, B: :success)).to eq :success
        end
      end
    end

    context 'with two tasks' do
      let(:plan) { parallel { task :A; task :B } }
      it_behaves_like 'multiple steps in parallel'
    end

    context 'with one task and one serial' do
      let(:plan) { parallel { task :A; serial { task :B } } }
      it_behaves_like 'multiple steps in parallel'
    end
  end

  context 'with composed serial and parallel' do
    let(:plan) do
      serial do
        parallel do
          task :A
          task :B
          serial do
            task :C
            task :D
          end
          parallel do
            task :E
            serial do
              task :F1
              task :F2
            end
          end
        end
        task :G
      end
    end

    it 'has an initial state' do
      steps = plan.next
      expect(steps).to eq %i[A B C E F1]
      expect(plan.state).to eq :unstarted
    end

    it 'recommends based on a success state is successful' do
      expect(plan.next(A: :success)).to eq %i[B C E F1]
      expect(plan.next(
               A: :success,
               B: :success,
               C: :success,
               E: :success
             )).to eq %i[D F1]
    end

    it 'recommends steps if something fails' do
      expect(plan.next(A: :failed)).to eq %i[B C E F1]
      expect(plan.next(
               A: :success,
               B: :success,
               C: :success,
               E: :failed
             )).to eq %i[D F1]
    end

    it 'recommends steps if something is pending' do
      expect(plan.next(A: :pending)).to eq %i[B C E F1]
      expect(plan.next(
               A: :success,
               B: :success,
               C: :success,
               E: :pending
             )).to eq %i[D F1]
    end

    it 'recommends the last serial step if everything is successful' do
      expect(plan.next(
               A: :success,
               B: :success,
               C: :success,
               D: :success,
               E: :success,
               F1: :success,
               F2: :success
             )).to eq [:G]
    end
  end

  context 'with failure action' do
    context 'for a serial plan' do
      let(:plan) do
        serial do
          task :A
          task :B
          failure do
            task :C
          end
        end
      end

      it 'does not run the failure on success' do
        expect(plan.next(A: :success, B: :success)).to be_empty
      end

      it 'does run failure on a failing task' do
        expect(plan.next(A: :failed)).to eq [:C]
        expect(plan.next(A: :success, B: :failed)).to eq [:C]
      end

      it 'has a failure state for the plan' do
        expect(plan.state(A: :failed)).to eq :failed
        expect(plan.state(A: :success, B: :failed)).to eq :failed
        expect(plan.state(A: :success, B: :failed, C: :success)).to eq :failed
      end
    end

    context 'for a parallel plan' do
      let(:plan) do
        parallel do
          task :A
          task :B
          failure do
            task :C
          end
        end
      end

      it 'does not run the failure on success' do
        expect(plan.next(A: :success, B: :success)).to be_empty
      end

      it 'does run failure on a failing task' do
        expect(plan.next(A: :failed)).to eq [:B]
        expect(plan.next(A: :success, B: :failed)).to eq [:C]
        expect(plan.next(A: :failed, B: :success)).to eq [:C]
      end

      it 'has a failure state for the plan' do
        expect(plan.state(A: :failed)).to eq :unstarted
        expect(plan.state(A: :success, B: :failed)).to eq :failed
        expect(plan.state(A: :failed, B: :success)).to eq :failed
        expect(plan.state(A: :success, B: :failed, C: :success)).to eq :failed
      end
    end
  end

  context 'with success action' do
    context 'for a serial plan' do
      let(:plan) do
        serial do
          task :A
          task :B
          success do
            task :C
          end
        end
      end

      it 'runs the success on all steps being successful' do
        expect(plan.next(A: :success, B: :success)).to eq [:C]
      end

      it 'returns state of the success step' do
        expect(plan.state(A: :success, B: :success, C: :success)).to eq :success
        expect(plan.state(A: :success, B: :success, C: :failed)).to eq :failed
      end

      it 'does not run success on a failing task' do
        expect(plan.next(A: :failed)).to eq []
        expect(plan.next(A: :success, B: :failed)).to eq []
      end
    end

    context 'for a parallel plan' do
      let(:plan) do
        parallel do
          task :A
          task :B
          success do
            task :C
          end
        end
      end

      it 'runs the success on all steps being successful' do
        expect(plan.next(A: :success, B: :success)).to eq [:C]
      end

      it 'returns state of of the success step' do
        expect(plan.state(A: :success, B: :success, C: :success)).to eq :success
        expect(plan.state(A: :success, B: :success, C: :failed)).to eq :failed
      end

      it 'does not run success on a failing task' do
        expect(plan.next(A: :failed)).to eq [:B]
        expect(plan.next(A: :success, B: :failed)).to eq []
      end
    end
  end

  context 'with a try action' do
    context 'for a serial plan' do
      let(:plan) do
        serial do
          task :A
          try do
            task :B
          end
        end
      end

      it 'recommends the try on previous step success' do
        expect(plan.next).to eq [:A]
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.next(A: :success, B: :success)).to eq []
      end

      it 'does not affect the success of the serial' do
        expect(plan.state(A: :success, B: :failed)).to eq :success
      end

      it 'does not affects the pending of the serial' do
        expect(plan.state(A: :success, B: :pending)).to eq :pending
      end
    end

    context 'for a parallel plan' do
      let(:plan) do
        parallel do
          task :A
          try do
            task :B
          end
        end
      end

      it 'recommends the try on previous step success' do
        expect(plan.next).to eq %i[A B]
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.next(A: :success, B: :success)).to eq []
      end

      it 'does not affect the success of the serial' do
        expect(plan.state(A: :success, B: :failed)).to eq :success
      end

      it 'does not affects the pending of the serial' do
        expect(plan.state(A: :success, B: :pending)).to eq :pending
      end
    end
  end

  context 'with finally action' do
    context 'for a serial plan' do
      let(:plan) do
        serial do
          task :A
          finally do
            task :B
          end
        end
      end

      it 'does not run finally until ready' do
        expect(plan.next).to eq [:A]
        expect(plan.next(A: :success, B: :success)).to eq []
      end

      it 'returns state of the finally step' do
        expect(plan.state(A: :success, B: :success)).to eq :success
        expect(plan.state(A: :success, B: :failed)).to eq :failed
      end

      it 'runs after any finishing state of the previous task' do
        expect(plan.next(A: :pending)).to eq []
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.next(A: :failed)).to eq [:B]
      end
    end

    context 'for a parallel plan' do
      let(:plan) do
        parallel do
          task :A
          finally do
            task :B
          end
        end
      end

      it 'does not run finally until ready' do
        expect(plan.next).to eq [:A]
        expect(plan.next(A: :success, B: :success)).to eq []
      end

      it 'returns state of the finally step' do
        expect(plan.state(A: :success, B: :success)).to eq :success
        expect(plan.state(A: :success, B: :failed)).to eq :failed
      end

      it 'runs after any finishing state of the previous task' do
        expect(plan.next(A: :pending)).to eq []
        expect(plan.next(A: :success)).to eq [:B]
        expect(plan.next(A: :failed)).to eq [:B]
      end
    end
  end

  context 'when defining success/failure with a finally' do
    context 'on a serial plan' do
      let(:plan) do
        serial do
          task :A
          success do
            task :B1
          end
          failure do
            task :B2
          end
          finally do
            task :C
          end
        end
      end

      it 'recommends success before the finally' do
        expect(plan.next(A: :success)).to eq [:B1]
        expect(plan.next(A: :success, B1: :success)).to eq [:C]
        expect(plan.next(A: :success, B1: :failed)).to eq [:C]
      end

      it 'recommends failure before the finally' do
        expect(plan.next(A: :failed)).to eq [:B2]
        expect(plan.next(A: :failed, B2: :success)).to eq [:C]
        expect(plan.next(A: :failed, B2: :failed)).to eq [:C]
      end
    end

    context 'on a parallel plan' do
      let(:plan) do
        parallel do
          task :A
          success do
            task :B1
          end
          failure do
            task :B2
          end
          finally do
            task :C
          end
        end
      end

      it 'recommends success before the finally' do
        expect(plan.next(A: :success)).to eq [:B1]
        expect(plan.next(A: :success, B1: :success)).to eq [:C]
        expect(plan.next(A: :success, B1: :failed)).to eq [:C]
      end

      it 'recommends failure before the finally' do
        expect(plan.next(A: :failed)).to eq [:B2]
        expect(plan.next(A: :failed, B2: :success)).to eq [:C]
        expect(plan.next(A: :failed, B2: :failed)).to eq [:C]
      end
    end
  end
end
