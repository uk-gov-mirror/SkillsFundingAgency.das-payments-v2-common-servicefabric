using System;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.Payments.EarningEvents.Messages.Events;
using SFA.DAS.Payments.Messages.Common.Events;
using SFA.DAS.Payments.Model.Core;
using SFA.DAS.Payments.ServiceFabric.Core.Messaging;

namespace SFA.DAS.Payments.ServiceFabric.Core.UnitTests.Messaging
{
    [TestFixture]
    public class EarningEventKeyTests
    {
        [Test]
        public void Key_Does_Not_Include_EventId_For_SLD_Events()
        {
            var earningEvent = CreateEvent<ApprenticeshipContractType1EarningEvent>();
            earningEvent.JobId = 123456; // Simulate an SLD event by setting JobId to a non-zero value.  The FundingPlatform should really be added to the base PaymentsEvent
            var earningEventKey = new EarningEventKey(earningEvent);
            earningEventKey.Key.Should().NotContain(earningEvent.EventId.ToString());

        }

        [Test]
        public void Key_Includes_EventId_For_AS_Events()
        {
            var earningEvent = CreateEvent<ApprenticeshipContractType1EarningEvent>();
            earningEvent.JobId = 0; // Simulate an AS event by setting JobId to 0.  The FundingPlatform should really be added to the base PaymentsEvent
            var earningEventKey = new EarningEventKey(earningEvent);
            earningEventKey.Key.Should().Contain(earningEvent.EventId.ToString());
        }

        [Test]
        public void Key_Includes_Course_Code()
        {
            var earningEvent = CreateEvent<ApprenticeshipContractType1EarningEvent>();
            earningEvent.LearningAim.CourseCode = "SCZ1234";
            var earningEventKey = new EarningEventKey(earningEvent);
            earningEventKey.Key.Should().Contain(earningEvent.LearningAim.CourseCode);
        }

        [Test]
        public void Key_Generation_Does_Not_Fail_For_Null_CourseCodes()
        {
            var earningEvent = CreateEvent<ApprenticeshipContractType1EarningEvent>();
            earningEvent.LearningAim.CourseCode = null;             
            Assert.DoesNotThrow(() => new EarningEventKey(earningEvent));
        }

        private T CreateEvent<T>() where T : PaymentsEvent, new()
        {
            return new T
            {
                EventId = Guid.NewGuid(),
                JobId = 123456,
                CollectionPeriod = new CollectionPeriod { AcademicYear = 2021, Period = 1 },
                Ukprn = 1234,
                EventTime = DateTimeOffset.UtcNow,
                IlrFileName = "test-filename1.xml",
                IlrSubmissionDateTime = DateTime.Now,
                Learner = new Learner
                {
                    Uln = 12345678,
                    ReferenceNumber = "learn-ref"
                },
                LearningAim = new LearningAim
                {
                    StartDate = DateTime.Now.AddYears(-1),
                    FrameworkCode = 1,
                    FundingLineType = "funding-line",
                    PathwayCode = 2,
                    ProgrammeType = 3,
                    Reference = "aim-ref",
                    SequenceNumber = 4,
                    StandardCode = 5,
                    CourseCode = "course-code",
                }
            };
        }

        private ApprenticeshipContractType1EarningEvent CreateDefaultEarningEvent() => CreateEvent<ApprenticeshipContractType1EarningEvent>();
    }
}